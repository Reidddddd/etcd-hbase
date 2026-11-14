package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/client"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/compare"
	"github.com/Reidddddd/etcd-hbase/testkit/go/internal/scenario"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if ctx == nil || stdout == nil || stderr == nil || len(arguments) == 0 {
		return errors.New("M4 driver requires a command and process streams")
	}
	switch arguments[0] {
	case "run":
		return runLeaseScenario(ctx, arguments[1:], stdout, stderr)
	case "compare":
		return compareLeaseTraces(arguments[1:], stdout, stderr)
	default:
		return errors.New("M4 driver command must be run or compare")
	}
}

type runOptions struct {
	endpoint   string
	nativeData string
	scenario   string
	scope      string
	prefix     string
	tracePath  string
	metadata   string
	layout     string
	timeout    time.Duration
}

func runLeaseScenario(parent context.Context, arguments []string, stdout, stderr io.Writer) (runErr error) {
	options, err := parseRunOptions(arguments, stderr)
	if err != nil {
		return err
	}
	suite, suiteDigest, err := scenario.LoadM4LeaseSuite(options.scenario)
	if err != nil {
		return err
	}
	build, err := scenario.BuildMetadata(suite)
	if err != nil {
		return errors.New("M4 pinned build metadata is unavailable")
	}
	if options.nativeData == "" {
		build.Distribution = "remote endpoint; Go build metadata does not attest the server version"
	}
	evidence, err := openEvidence(options, build, suiteDigest, stdout)
	if err != nil {
		return err
	}
	defer func() { runErr = evidence.finish(runErr) }()

	ctx, cancel := context.WithTimeout(parent, options.timeout)
	defer cancel()
	if options.nativeData != "" {
		return scenario.RunM4NativeLease(ctx, options.nativeData, options.prefix, evidence.trace)
	}
	connection, err := client.New(client.Options{Endpoint: options.endpoint, Timeout: options.timeout})
	if err != nil {
		return errors.New("M4 target client initialization failed")
	}
	defer func() { runErr = errors.Join(runErr, connection.Close()) }()
	return scenario.RunM4Lease(ctx, connection, options.scope, options.prefix, evidence.trace)
}

func parseRunOptions(arguments []string, stderr io.Writer) (runOptions, error) {
	flags := flag.NewFlagSet("m4driver run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var options runOptions
	flags.StringVar(&options.endpoint, "endpoint", "", "explicit etcd-compatible endpoint")
	flags.StringVar(&options.nativeData, "native-data-dir", "", "new isolated embedded etcd data directory")
	flags.StringVar(&options.scenario, "scenario", "", "registered pinned M4 scenario manifest")
	flags.StringVar(&options.scope, "scope", "", "native-etcd or target-hbase")
	flags.StringVar(&options.layout, "layout", "", "target layout HISTORY or NATIVE")
	flags.StringVar(&options.prefix, "prefix", "", "fresh isolated M4 test prefix")
	flags.StringVar(&options.tracePath, "trace", "", "new JSONL trace path")
	flags.StringVar(&options.metadata, "metadata", "", "new JSON evidence metadata path")
	flags.DurationVar(&options.timeout, "timeout", 2*time.Minute, "whole-scenario deadline")
	if err := flags.Parse(arguments); err != nil {
		return runOptions{}, err
	}
	if flags.NArg() != 0 || options.scenario == "" || options.tracePath == "" ||
		options.metadata == "" || options.timeout <= 0 || options.timeout > 5*time.Minute {
		return runOptions{}, errors.New("M4 run requires scenario, trace, metadata, and a deadline up to five minutes")
	}
	if options.nativeData != "" {
		if options.endpoint != "" || options.scope != "native-etcd" || options.layout != "" {
			return runOptions{}, errors.New("embedded M4 mode requires native-etcd scope without endpoint or layout")
		}
	} else {
		if err := (client.Options{Endpoint: options.endpoint, Timeout: options.timeout}).Validate(); err != nil {
			return runOptions{}, err
		}
		if options.layout != "HISTORY" && options.layout != "NATIVE" {
			return runOptions{}, errors.New("M4 target mode requires an explicit HISTORY or NATIVE layout")
		}
	}
	if err := scenario.ValidateM4Lease(options.scope, options.prefix); err != nil {
		return runOptions{}, err
	}
	if (options.scope == "native-etcd") != (options.nativeData != "") {
		return runOptions{}, errors.New("M4 evidence scope does not match the selected endpoint mode")
	}
	return options, nil
}

type runMetadata struct {
	Schema         string            `json:"schema"`
	RunID          string            `json:"run_id,omitempty"`
	SourceCommit   string            `json:"source_commit,omitempty"`
	Build          scenario.Metadata `json:"build"`
	SuiteSHA256    string            `json:"suite_sha256"`
	EvidenceScope  string            `json:"evidence_scope"`
	PrefixSHA256   string            `json:"prefix_sha256"`
	TraceSHA256    string            `json:"trace_sha256"`
	EmbeddedNative bool              `json:"embedded_native"`
	Layout         string            `json:"layout"`
	Status         string            `json:"status"`
}

type evidenceOutput struct {
	options     runOptions
	build       scenario.Metadata
	suiteSHA256 string
	stdout      io.Writer
	trace       io.Writer
	traceHash   hash.Hash
	closeTrace  func() error
	metadata    *os.File
}

func openEvidence(options runOptions, build scenario.Metadata, suiteSHA256 string,
	stdout io.Writer) (*evidenceOutput, error) {
	if filepath.Clean(options.tracePath) == filepath.Clean(options.metadata) {
		return nil, errors.New("M4 trace and metadata paths must differ")
	}
	if err := os.MkdirAll(filepath.Dir(options.tracePath), 0o700); err != nil {
		return nil, errors.New("M4 trace directory could not be created")
	}
	if err := os.MkdirAll(filepath.Dir(options.metadata), 0o700); err != nil {
		return nil, errors.New("M4 metadata directory could not be created")
	}
	traceFile, err := os.OpenFile(options.tracePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, errors.New("M4 trace output could not be opened exclusively")
	}
	metadataFile, err := os.OpenFile(options.metadata, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, errors.Join(errors.New("M4 metadata output could not be opened exclusively"), traceFile.Close())
	}
	traceHash := sha256.New()
	return &evidenceOutput{options: options, build: build, suiteSHA256: suiteSHA256, stdout: stdout,
		trace: io.MultiWriter(traceFile, traceHash), traceHash: traceHash,
		closeTrace: traceFile.Close, metadata: metadataFile}, nil
}

func (output *evidenceOutput) finish(runErr error) error {
	closeErr := output.closeTrace()
	if closeErr != nil {
		runErr = errors.Join(runErr, closeErr)
	}
	status := "passed"
	if runErr != nil {
		status = "failed"
	}
	prefixHash := sha256.Sum256([]byte(output.options.prefix))
	metadata := runMetadata{
		Schema: "etcd-hbase-m4-run/v1", RunID: os.Getenv("M4_RUN_ID"),
		SourceCommit: os.Getenv("M4_SOURCE_COMMIT"), Build: output.build,
		SuiteSHA256: output.suiteSHA256, EvidenceScope: output.options.scope,
		PrefixSHA256: hex.EncodeToString(prefixHash[:]), TraceSHA256: hex.EncodeToString(output.traceHash.Sum(nil)),
		EmbeddedNative: output.options.nativeData != "", Layout: output.options.layout, Status: status,
	}
	encodeErr := json.NewEncoder(output.metadata).Encode(metadata)
	closeMetadataErr := output.metadata.Close()
	if encodeErr != nil || closeMetadataErr != nil {
		runErr = errors.Join(runErr, errors.New("M4 metadata could not be finalized"), encodeErr, closeMetadataErr)
	}
	if runErr == nil && output.options.tracePath != "-" {
		_, runErr = fmt.Fprintln(output.stdout, "M4 Lease scenario passed")
	}
	return runErr
}

func compareLeaseTraces(arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("m4driver compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var left, right string
	flags.StringVar(&left, "left", "", "native trace JSONL")
	flags.StringVar(&right, "right", "", "target trace JSONL")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || left == "" || right == "" || left == right {
		return errors.New("M4 compare requires distinct left and right trace files")
	}
	leftFile, err := os.Open(left)
	if err != nil {
		return errors.New("native M4 trace cannot be opened")
	}
	defer leftFile.Close()
	rightFile, err := os.Open(right)
	if err != nil {
		return errors.New("target M4 trace cannot be opened")
	}
	defer rightFile.Close()
	if err := compare.CompareM4LeaseTraces(leftFile, rightFile); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, "M4 Lease traces match the registered native comparison policy")
	return err
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"io.etcdhbase/testkit/internal/compare"
	"io.etcdhbase/testkit/internal/scenario"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: native-etcd run|compare|normalize [options]")
	}
	switch args[0] {
	case "run":
		return runScenario(args[1:], stdout, stderr)
	case "compare":
		return compareTraces(args[1:], stderr)
	case "normalize":
		return normalizeTrace(args[1:], stderr)
	default:
		return errors.New("usage: native-etcd run|compare|normalize [options]")
	}
}

func normalizeTrace(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("native-etcd normalize", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPath := flags.String("input", "", "complete input trace JSONL")
	outputPath := flags.String("output", "", "normalized evidence JSONL output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *inputPath == "" || *outputPath == "" {
		return errors.New("normalize requires --input and --output")
	}
	input, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("open input trace: %w", err)
	}
	defer input.Close()
	output, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("open normalized output: %w", err)
	}
	normalizeErr := compare.NormalizeTrace(input, output)
	closeErr := output.Close()
	if normalizeErr != nil {
		return normalizeErr
	}
	if closeErr != nil {
		return fmt.Errorf("close normalized output: %w", closeErr)
	}
	return nil
}

func runScenario(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("native-etcd run", flag.ContinueOnError)
	flags.SetOutput(stderr)
	scenarioPath := flags.String("scenario", "../scenarios/m0-native.yaml", "pinned native scenario manifest")
	artifactRoot := flags.String("artifact-root", "", "ignored directory for all native etcd data and outputs")
	tracePath := flags.String("trace", "", "JSONL request/response trace output")
	metadataPath := flags.String("metadata", "", "JSON build and distribution metadata output")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *artifactRoot == "" || *tracePath == "" || *metadataPath == "" {
		return errors.New("run requires --artifact-root, --trace, and --metadata")
	}
	root, err := filepath.Abs(*artifactRoot)
	if err != nil {
		return fmt.Errorf("resolve artifact root: %w", err)
	}
	traceFilePath, err := insideRoot(root, *tracePath)
	if err != nil {
		return err
	}
	metadataFilePath, err := insideRoot(root, *metadataPath)
	if err != nil {
		return err
	}
	dataDirectory, err := insideRoot(root, "etcd-data")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create native artifact root: %w", err)
	}
	suite, err := scenario.LoadSuite(*scenarioPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(traceFilePath), 0o700); err != nil {
		return fmt.Errorf("create trace directory: %w", err)
	}
	traceFile, err := os.OpenFile(traceFilePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("open trace output: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metadata, runErr := scenario.Run(ctx, suite, dataDirectory, traceFile)
	closeErr := traceFile.Close()
	if closeErr != nil {
		return fmt.Errorf("close trace output: %w", closeErr)
	}
	if metadata.ScenarioID != "" {
		if err := writeMetadata(metadataFilePath, metadata); err != nil {
			return err
		}
	}
	if runErr != nil {
		return runErr
	}
	if _, err := fmt.Fprintf(stdout, "native scenario %s passed; trace=%s metadata=%s\n", suite.ID, traceFilePath, metadataFilePath); err != nil {
		return err
	}
	return nil
}

func compareTraces(args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("native-etcd compare", flag.ContinueOnError)
	flags.SetOutput(stderr)
	leftPath := flags.String("left", "", "left complete trace JSONL")
	rightPath := flags.String("right", "", "right complete trace JSONL")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *leftPath == "" || *rightPath == "" {
		return errors.New("compare requires --left and --right")
	}
	left, err := os.Open(*leftPath)
	if err != nil {
		return fmt.Errorf("open left trace: %w", err)
	}
	defer left.Close()
	right, err := os.Open(*rightPath)
	if err != nil {
		return fmt.Errorf("open right trace: %w", err)
	}
	defer right.Close()
	if err := compare.CompareTraces(left, right); err != nil {
		return err
	}
	return nil
}

func insideRoot(root, path string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve artifact root: %w", err)
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve artifact path: %w", err)
	}
	relative, err := filepath.Rel(root, absolute)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || len(relative) > 2 && relative[:3] == ".."+string(filepath.Separator) {
		return "", errors.New("trace and metadata outputs must remain inside artifact root")
	}
	current := root
	components := strings.Split(relative, string(filepath.Separator))
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			break
		}
		if err != nil {
			return "", fmt.Errorf("inspect artifact path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("trace, metadata, and data paths must not traverse symlinks")
		}
		if index < len(components)-1 && !info.IsDir() {
			return "", errors.New("trace, metadata, and data paths must use directory components")
		}
	}
	return absolute, nil
}

func writeMetadata(path string, metadata scenario.Metadata) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create metadata directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("open metadata output: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encodeErr := encoder.Encode(metadata)
	closeErr := file.Close()
	if encodeErr != nil {
		return fmt.Errorf("write metadata output: %w", encodeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close metadata output: %w", closeErr)
	}
	return nil
}

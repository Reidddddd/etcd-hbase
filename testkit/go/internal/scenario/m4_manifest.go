package scenario

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"

	"sigs.k8s.io/yaml"
)

var expectedM4LeaseSteps = []Step{
	{CaseID: "L05", ID: "native-baseline"},
	{CaseID: "L01", ID: "auto-grant"},
	{CaseID: "L01", ID: "auto-ttl"},
	{CaseID: "L01", ID: "explicit-grant"},
	{CaseID: "L01", ID: "explicit-ttl"},
	{CaseID: "L01", ID: "duplicate-id"},
	{CaseID: "L04", ID: "missing-ttl"},
	{CaseID: "L01", ID: "negative-id"},
	{CaseID: "L05", ID: "leases-list"},
	{CaseID: "L01", ID: "oversized-ttl"},
	{CaseID: "L04", ID: "zero-id-ttl"},
}

// LoadM4LeaseSuite accepts only the pinned J40 public lease-query scenario.
func LoadM4LeaseSuite(path string) (Suite, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return Suite{}, "", errors.New("M4 lease scenario manifest cannot be opened")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 65_537))
	if err != nil || len(data) == 0 || len(data) > 65_536 {
		return Suite{}, "", errors.New("M4 lease scenario manifest is empty or oversized")
	}
	var suite Suite
	if err := yaml.UnmarshalStrict(data, &suite); err != nil {
		return Suite{}, "", errors.New("M4 lease scenario manifest is invalid")
	}
	if suite.SchemaVersion != 1 || suite.ID != "m4-lease-v1" || suite.Protocol != protocolVersion ||
		len(suite.Steps) != len(expectedM4LeaseSteps) {
		return Suite{}, "", errors.New("M4 lease scenario identity or coverage is not pinned")
	}
	for index, expected := range expectedM4LeaseSteps {
		if suite.Steps[index] != expected {
			return Suite{}, "", errors.New("M4 lease scenario coverage differs from its pinned steps")
		}
	}
	digest := sha256.Sum256(data)
	return suite, hex.EncodeToString(digest[:]), nil
}

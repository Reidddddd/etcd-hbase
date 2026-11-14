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

var expectedM4LeaseBindingSteps = []Step{
	{CaseID: "L05", ID: "binding-baseline"},
	{CaseID: "L01", ID: "binding-grant-a"},
	{CaseID: "L01", ID: "binding-grant-b"},
	{CaseID: "T01", ID: "binding-register-then"},
	{CaseID: "L04", ID: "binding-ttl-a-initial"},
	{CaseID: "L04", ID: "binding-ttl-b-initial"},
	{CaseID: "P01", ID: "binding-ignore-flags"},
	{CaseID: "L04", ID: "binding-ttl-a-preserved"},
	{CaseID: "P01", ID: "binding-rebind-a-to-b"},
	{CaseID: "L04", ID: "binding-ttl-a-after-rebind"},
	{CaseID: "L04", ID: "binding-ttl-b-after-rebind"},
	{CaseID: "T01", ID: "binding-register-else"},
	{CaseID: "L04", ID: "binding-ttl-b-after-else"},
	{CaseID: "T01", ID: "binding-register-nested"},
	{CaseID: "L04", ID: "binding-ttl-a-after-nested"},
	{CaseID: "L04", ID: "binding-ttl-b-after-nested"},
	{CaseID: "P01", ID: "binding-unbind"},
	{CaseID: "L04", ID: "binding-ttl-a-after-unbind"},
	{CaseID: "D01", ID: "binding-delete"},
	{CaseID: "L04", ID: "binding-ttl-b-after-delete"},
	{CaseID: "P01", ID: "binding-recreate-unleased"},
	{CaseID: "L04", ID: "binding-ttl-b-after-recreate"},
	{CaseID: "D01", ID: "binding-delete-range"},
	{CaseID: "L04", ID: "binding-ttl-a-after-delete-range"},
	{CaseID: "L04", ID: "binding-ttl-b-after-delete-range"},
	{CaseID: "R01", ID: "binding-role-prefix-after-delete-range"},
	{CaseID: "R01", ID: "binding-version-after-delete-range"},
	{CaseID: "P01", ID: "binding-survivor-put"},
	{CaseID: "L04", ID: "binding-ttl-b-after-survivor-put"},
	{CaseID: "T01", ID: "binding-invalid-lease-atomicity"},
	{CaseID: "R01", ID: "binding-atomicity-range-first"},
	{CaseID: "R01", ID: "binding-atomicity-range-second"},
	{CaseID: "L04", ID: "binding-ttl-a-after-failed-txn"},
	{CaseID: "P01", ID: "binding-invalid-lease-put"},
	{CaseID: "R01", ID: "binding-atomicity-range-invalid-put"},
	{CaseID: "T01", ID: "binding-invalid-lease-unselected-branch"},
	{CaseID: "R01", ID: "binding-unselected-then-absent"},
	{CaseID: "R01", ID: "binding-unselected-else-visible"},
	{CaseID: "L04", ID: "binding-ttl-b-after-unselected-branch"},
}

// LoadM4LeaseSuite accepts only the pinned J40 public lease-query scenario.
func LoadM4LeaseSuite(path string) (Suite, string, error) {
	return loadM4Suite(path, "m4-lease-v1", expectedM4LeaseSteps)
}

// LoadM4LeaseBindingSuite accepts only the pinned J41 lease-binding scenario.
func LoadM4LeaseBindingSuite(path string) (Suite, string, error) {
	return loadM4Suite(path, "m4-lease-bindings-v1", expectedM4LeaseBindingSteps)
}

func loadM4Suite(path, expectedID string, expectedSteps []Step) (Suite, string, error) {
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
	if suite.SchemaVersion != 1 || suite.ID != expectedID || suite.Protocol != protocolVersion ||
		len(suite.Steps) != len(expectedSteps) {
		return Suite{}, "", errors.New("M4 lease scenario identity or coverage is not pinned")
	}
	for index, expected := range expectedSteps {
		if suite.Steps[index] != expected {
			return Suite{}, "", errors.New("M4 lease scenario coverage differs from its pinned steps")
		}
	}
	digest := sha256.Sum256(data)
	return suite, hex.EncodeToString(digest[:]), nil
}

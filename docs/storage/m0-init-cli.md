# M0 HBase table initialization CLI

`scripts/dev/m0-admin.sh` exposes the M0 table-group planning and initialization prototype. It reads a validated runtime configuration and supports the two explicit history-layout candidates in `config/examples/m0/`.

Set `ETCD_HBASE_JDK21_HOME` to the pinned Temurin JDK 21 installation before running the script. The script compiles the admin module with the repository's locked Maven wrapper before starting the CLI.

## Plan a table group

```bash
scripts/dev/m0-admin.sh init plan \
  --config config/examples/m0/history-table.yaml
```

The command prints the selected HBase profile, namespace, schema versions, column families, salt profile, history layout, and split keys. It also prints the configured table names in that physical HBase target and namespace, plus a SHA-256 confirmation fingerprint. The namespace list is a safety scope: it includes tables belonging to other configured groups that share the same canonical target, but those sibling tables are not modified by this plan. Planning does not open a connection to HBase. If a configuration contains multiple table groups, pass `--table-group ID`. If its history layout is undecided, select a configured candidate with `--history-layout`.

Review the complete plan and confirm both the table-group ID and fingerprint before applying it:

```bash
scripts/dev/m0-admin.sh init apply \
  --config config/examples/m0/history-table.yaml \
  --confirm-table-group local \
  --confirm-plan sha256:<fingerprint-from-plan>
```

The fingerprint changes when the selected profile, schema, split keys, column families, history layout, or configured table names in the same namespace change. A stale fingerprint is rejected before the command opens an HBase connection. Apply currently supports only the isolated `DEV_NO_AUTH` development provider; other identity modes report missing input.

## Apply behavior

The initializer creates the configured namespace if needed, then creates only the tables in the selected group. Tables from sibling groups configured for the same physical target and namespace are allowed and left untouched. It refuses to continue when the namespace contains any other table, a selected-group table has a different descriptor or split boundaries, or a partially created selected group already contains rows. If creation stops partway through, rerun the same command with the same fingerprint after checking the partial result. Matching empty tables are retained and the remaining selected-group tables can be created. When the full selected group already matches, the command returns `already-initialized` without changing it.

The prototype does not delete tables, namespaces, or rows. It does not choose a history layout implicitly. The standalone history table and native HBase cell-version candidates are both prototypes; selecting a candidate is not a production schema decision. Production bootstrap, recovery, and schema upgrades remain part of J55/J57.

This CLI and the `m0-hbase-local` profile provide public local evidence only. They do not establish target-cluster authentication, authorization, Java/HBase compatibility, or operational readiness. Target apply requires the missing target identity and environment inputs to be supplied separately.

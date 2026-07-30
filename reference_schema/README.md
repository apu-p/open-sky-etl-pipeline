# Schema-drift references

The schema-drift audit (Section 12, check #2 in `etl/audit/checks.py`) compares
each source's raw JSON top-level keys against a reference stored here as
`{name}.json` (e.g. `open_meteo.json`, `donki_CME.json`, `neows.json`).

The first time a source is seen, the check **bootstraps** the reference from the
observed keys and records an `info` finding. From then on, missing or added
top-level keys surface as an `audit_findings` row (and, combined with the
fail-fast load, a hard DAG failure if the shape actually broke the transform).

Generated `*.json` references are environment-specific and gitignored — only
this README is tracked.

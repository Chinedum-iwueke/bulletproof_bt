# Incident taxonomy

Severities:
- `info`
- `warning`
- `error`
- `critical`

Taxonomy values:
- `startup`
- `auth`
- `transport`
- `stream_health`
- `reconcile`
- `lifecycle`
- `canary`
- `freeze`
- `recovery`
- `config`
- `doctor`

Each incident record includes UTC `ts`, `run_id`, `incident_type`, `taxonomy`, `severity`, `message`, and optional `context`.

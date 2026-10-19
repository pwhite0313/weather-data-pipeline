# Security: secret scan and remediation

The repository is public. On 2026-10-19 the full git history was scanned with
[gitleaks](https://github.com/gitleaks/gitleaks), the findings were rotated, and
the file that held them was removed from tracking. This page records what was
found, what each value actually protected, what was done, and why history was
kept.

## What was found

`gitleaks git -v --redact` (v8.30.1) over 146 commits found three secrets, all
in `config/airflow.cfg`, all from commit `f860cad` (2026-03-19):

| Line | Setting | Purpose |
|---|---|---|
| 165 | `[core] fernet_key` | Encrypts connections and variables in the metadata DB |
| 1204 | `[api] secret_key` | Signs web UI sessions (Airflow 3) |
| 1510 | `[api_auth] jwt_secret` | Signs API tokens (Airflow 3) |

An earlier scan on 2026-10-17 reported two findings (lines 165 and 1510); this
scan also flags line 1204. All three are treated as leaked.

Nothing else was found: no OpenWeather key, no AWS credentials, no private key.
`.env` and `*.p8` files were never committed in any branch; only `.env.example`,
which holds no values.

## How the file got there

In March the project ran the official Airflow docker-compose template, with the
`apache/airflow:3.1.8` image. That template mounts `./config` into the container
at `/opt/airflow/config` and points `AIRFLOW_CONFIG` there. On first start the
init container ran `airflow config list`, which writes a default `airflow.cfg`
with freshly generated secrets. Because of the mount, the file landed in the
repository and was committed. (`dags_folder = /opt/airflow/dags` in the file
confirms it was generated inside a container.)

The project later moved to its own image on Airflow 2.8.1 and dropped the
`config` mount. `airflow.cfg` was added to `.gitignore`, but the file was
already tracked, so it stayed in the repository.

## What each value actually protected

- **fernet_key: nothing.** The compose file set `AIRFLOW__CORE__FERNET_KEY: ''`,
  and Airflow environment variables take precedence over `airflow.cfg`. That
  stack ran without encryption, so nothing was ever encrypted with this key.
- **secret_key and jwt_secret: a local Airflow 3 stack, on localhost, in March.**
  Nothing overrode them, so they signed sessions and tokens for that stack while
  it existed. It no longer runs.
- **Today: nothing reads the file.** The current compose file does not mount
  `config/`, and Airflow 2.8.1 ignores both `[api]` settings.

So none of the three values unlocks anything. They are rotated anyway: a secret
that has been public is treated as burnt.

## What was done

1. **Checked the metadata DB.** `airflow connections list` and
   `airflow variables list` both returned no rows, so nothing was stored in
   plaintext under the old empty key and `airflow rotate-fernet-key` was not
   needed.
2. **Generated new values** directly into `.env` (gitignored), without printing
   them:
   - `AIRFLOW__CORE__FERNET_KEY`
   - `AIRFLOW__WEBSERVER__SECRET_KEY`: the setting 2.8.1 actually uses for the UI
     and for log fetching. It was not in the leaked file, but the stack had no
     explicit value, so it is set now.
   - `AIRFLOW__API__SECRET_KEY` and `AIRFLOW__API_AUTH__JWT_SECRET`: replacements
     for the leaked Airflow 3 values, ignored by 2.8.1, set so an upgrade does
     not start without them.
3. **Removed the override** `AIRFLOW__CORE__FERNET_KEY: ''` from
   `docker-compose.yml`. It sat under `environment:`, which wins over
   `env_file:`, so the key in `.env` would otherwise never have reached Airflow.
   Verified inside the running scheduler with
   `airflow config get-value core fernet_key` (correct length; value not printed).
4. **Stopped tracking the file**: `git rm config/airflow.cfg`. The existing
   `.gitignore` rule keeps it out from now on.
5. **Documented the variables** in `.env.example`, empty, with how to generate
   each one.

## History: kept, not rewritten

The old values are still readable at commit `f860cad`. History was not rewritten
with `git filter-repo`, because:

- The values are rotated and, as shown above, guard nothing. Rewriting history
  protects a secret that still works; these do not.
- The repository is already public and has been cloned. A rewrite cannot recall
  copies that already exist, so it would not make the old values any less
  public.
- A rewrite changes every commit hash after March, which breaks links to
  commits and the merged pull request.

The three fingerprints are listed in `.gitleaksignore`, with the rotation date,
so the scan is clean from here on and can run in CI.

## Scan output

Baseline, 2026-10-19, before remediation (`gitleaks git -v --redact`):

```
Finding:     fernet_key = REDACTED
Secret:      REDACTED
RuleID:      generic-api-key
Entropy:     4.896821
File:        config/airflow.cfg
Line:        165
Commit:      f860cadf59af27e0c7ac87ab0048de98ebc90087
Date:        2026-03-19T21:02:48Z
Fingerprint: f860cadf59af27e0c7ac87ab0048de98ebc90087:config/airflow.cfg:generic-api-key:165
Link:        https://github.com/pwhite0313/weather-data-pipeline/blob/f860cadf59af27e0c7ac87ab0048de98ebc90087/config/airflow.cfg#L165

Finding:     secret_key = REDACTED
Secret:      REDACTED
RuleID:      generic-api-key
Entropy:     4.105389
File:        config/airflow.cfg
Line:        1204
Commit:      f860cadf59af27e0c7ac87ab0048de98ebc90087
Date:        2026-03-19T21:02:48Z
Fingerprint: f860cadf59af27e0c7ac87ab0048de98ebc90087:config/airflow.cfg:generic-api-key:1204
Link:        https://github.com/pwhite0313/weather-data-pipeline/blob/f860cadf59af27e0c7ac87ab0048de98ebc90087/config/airflow.cfg#L1204

Finding:     jwt_secret = REDACTED
Secret:      REDACTED
RuleID:      generic-api-key
Entropy:     4.168296
File:        config/airflow.cfg
Line:        1510
Commit:      f860cadf59af27e0c7ac87ab0048de98ebc90087
Date:        2026-03-19T21:02:48Z
Fingerprint: f860cadf59af27e0c7ac87ab0048de98ebc90087:config/airflow.cfg:generic-api-key:1510
Link:        https://github.com/pwhite0313/weather-data-pipeline/blob/f860cadf59af27e0c7ac87ab0048de98ebc90087/config/airflow.cfg#L1510

INF 146 commits scanned.
INF scanned ~2130126 bytes (2.13 MB) in 400ms
WRN leaks found: 3
```

After remediation, with `.gitleaksignore` in place:

```
INF 146 commits scanned.
INF scanned ~2130126 bytes (2.13 MB) in 404ms
INF no leaks found
```

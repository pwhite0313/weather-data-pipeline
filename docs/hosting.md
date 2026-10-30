# Hosting

Where the pipeline runs when the laptop is off, and how to get at it.

## The box

| | |
|---|---|
| Instance | `i-06974584beba5796b`, name `weather-airflow` |
| Type | `t3.medium` (2 vCPU, 4 GB; Airflow's compose stack wants 4 GB, a `t3.small` swaps and stalls) |
| Image | Ubuntu Server 24.04 LTS, from the canonical SSM parameter at launch |
| Disk | 20 GB gp3 |
| Region | `us-east-2` (Ohio), same as the bucket and the Snowflake account |
| Public IP | In the EC2 console, not written here (the repo is public). Not an Elastic IP: a stop and start changes it. Public IPv4 is billed per hour either way (about $0.005, about $3.65 a month). |
| Created | 2026-10-29 by `infra/ec2/create_box.sh` from CloudShell |

Everything on this page except the budget and the IAM roles is in `us-east-2`.
The console opens in whichever region it last showed, and the Instances page
in another region is honestly empty: when the instance "is not there", check
the region selector (top right) first.

**Price:** `t3.medium` on demand in `us-east-2`, $0.0416 an hour on
2026-10-29 (from a third-party price aggregator; confirm on the AWS on-demand
page when the first bill arrives), about $30.40 for a 730 hour month. Plus
the disk (about $1.60 a month) and the public IPv4 (about $3.65): about $36
a month running all the time, which is why the budget is $40.

**Alarm:** `weather-airflow-status` in CloudWatch (Ohio), created 2026-10-30
in the console. `StatusCheckFailed` on the instance, maximum over 5 minute
periods, 2 of 2 periods at or above 1, email through the SNS topic
`weather-alarms` (subscription confirmed). The same topic is notified on
`INSUFFICIENT_DATA`, which is what a stopped or terminated instance looks
like: the metric stops arriving. This is the watcher outside the box for the
box itself; ntfy needs Airflow running to fire, and `RAW.WEATHER_STALE` does
not look until 12:00 UTC.

**Budget:** `weather-hosting`, $40 a month, email alerts at 50, 80 and 100
percent. A budget alerts; it does not stop anything.

## Access

- **Security group** `sg-00755a4be383bf5e7`: inbound port 22 from one home IP
  `/32`, nothing else. Port 8080 is never opened; the UI comes through the
  tunnel below.
- **When the home IP changes:** edit the existing rule in the security group to
  the new IP `/32`. Do not add `0.0.0.0/0` or widen the range.
- **SSH key:** `~/.ssh/weather-ec2` on the laptop (ed25519, generated there;
  only the public half was imported into AWS). Not in the repo.

```bash
# shell on the box
ssh -i ~/.ssh/weather-ec2 ubuntu@<box public ip>

# the Airflow UI at http://localhost:8080, only while this is open
ssh -i ~/.ssh/weather-ec2 -L 8080:localhost:8080 ubuntu@<box public ip>
```

UI login: `admin`, password in the box's `~/weather/.env` as
`AIRFLOW_WWW_PASSWORD` (`grep AIRFLOW_WWW_PASSWORD ~/weather/.env` on the box).

## Credentials on the box

- **AWS:** none. The instance role `weather-ec2-role` carries
  `infra/iam/extractor_write_policy.json` (PutObject under `weather/*`,
  ListBucket on that prefix). boto3 inside the containers finds it through the
  instance metadata service: `aws sts get-caller-identity` on the box and
  `boto3.client("sts").get_caller_identity()` in the scheduler container both
  return `assumed-role/weather-ec2-role/i-06974584beba5796b`.
- **Snowflake:** key pair as `WEATHER_SVC`, `~/weather/keys/rsa_key.p8`,
  copied by `scp`, mode 600.
- **The rest:** `~/weather/.env`, mode 600, copied by `scp` with the AWS key
  lines removed and fresh Airflow secrets (Fernet key, webserver secret key,
  UI password) generated for the box. The laptop's values were not reused.

## Running it

Setup was `infra/ec2/bootstrap_box.sh` (Docker Engine from Docker's apt
repository, UTC, the clone to `~/weather`), then the secrets by `scp`, then:

```bash
cd ~/weather
docker compose build       # about 10 minutes on a t3.medium
docker compose up -d
docker compose ps          # every service healthy
```

Every service has `restart: always`, so a reboot brings the stack back
without a login. The DAG stays in whatever paused state it had.

## Reading logs

```bash
docker compose logs --since 24h airflow-scheduler          # the scheduler
docker compose exec airflow-scheduler airflow dags list-runs -d weather_daily
ls ~/weather/logs/dag_id=weather_daily/                    # one folder per run, one file per task attempt
```

## Review

The hosted review (the guide's day 16) is the day after the tenth hosted
run. First hosted run: 2026-10-29 (manual). First scheduled: 2026-10-30 06:00
UTC. Tenth scheduled run: 2026-11-08. **Review date: 2026-11-09.**

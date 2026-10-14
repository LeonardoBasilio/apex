# deploy-cloud

Cloud deployment target for this portfolio port: [`terraform/aws/`](terraform/aws/)
provisions MSK + RDS Postgres as the managed equivalent of
[`store-postgres/docker-compose.yml`](../store-postgres/docker-compose.yml)'s local Kafka +
Postgres; [`databricks/job.json`](databricks/job.json) runs
[`dev-pyspark/`](../dev-pyspark/)'s already-verified skew-join job as a Databricks Job instead
of a local Docker container.

## What this is, precisely

**Infrastructure as code, reviewed and locally validated, never applied.** This matters
enough to state plainly, the same way the rest of this port states what's deferred rather
than letting it read as done:

- `terraform/aws/*.tf` passed `terraform validate` (HCL syntax + type checking, entirely
  local — no AWS API calls) and `terraform fmt`. It was **not** run through `terraform plan`
  or `terraform apply` against any AWS account, including the one this session had live,
  working credentials for (`757312183402`) — provisioning real MSK/RDS resources has real
  cost and was explicitly declined for this branch.
- `databricks/job.json` is valid JSON and follows the Jobs API 2.1 shape, written by hand
  against the documented schema. It has **not** been submitted to a live Databricks
  workspace — no `databricks jobs create` was run, no workspace was available in this
  session.

Nothing here is a claim that this deploys cleanly on the first try. Real IaC rarely does;
what's written here hasn't had the chance to be wrong against a real account yet.

## terraform/aws/ — MSK + RDS

Maps the local stack's two stateful services onto managed AWS equivalents, in a dedicated VPC
(not the account default) so the whole stack is one `terraform destroy` away from gone:

| Local (`store-postgres/docker-compose.yml`) | AWS | Note |
|---|---|---|
| `kafka` (single-node KRaft) | `aws_msk_cluster` (2 brokers, `kafka.t3.small`) | PLAINTEXT client-broker encryption, matching `stream-kafka/`'s and `store-postgres/`'s clients as written today — neither configures TLS. A real deployment should turn this on in both places together, not just here. |
| `postgres` | `aws_db_instance` (`db.t3.micro`) | Not publicly accessible; `store-postgres/`'s own DDL still owns schema application, same division as the Compose stack. |

**What this is NOT:** `allowed_cidr_blocks` defaults to an empty list (nothing can reach
either service until a real deployment supplies it) — deliberately a placeholder, not a
real security posture. A real deployment should reference the actual consuming compute's
security group (an ECS task running `store-postgres/`'s consumer, or a Databricks VPC
endpoint), not a CIDR range at all.

```sh
cd terraform/aws
terraform init
terraform plan -var="db_password=$(openssl rand -base64 24)"   # review before ever applying
```

## databricks/job.json — running dev-pyspark/'s job on Databricks

Runs [`dev-pyspark/jobs/skew_join_job.py`](../dev-pyspark/jobs/skew_join_job.py)
**unmodified** — the exact script already verified end to end against the local stack (see
`dev-pyspark/README.md`). Only the environment changes:

- `init_scripts/install_apex_capture.sh` installs `capture-pyspark`/`stream-kafka` from
  wheels staged in a Unity Catalog volume, not PyPI (these are this branch's own unpublished
  lanes) — build with `python -m build` in each lane, then `databricks fs cp` to the volume
  path the script reads from (`APEX_WHEEL_DIR`).
- `job.json`'s `spark_env_vars.KAFKA_BOOTSTRAP_SERVERS` references a Databricks secret
  (`{{secrets/apex-portfolio/msk_bootstrap_brokers}}`) rather than a literal value — populate
  it from `terraform/aws/`'s `msk_bootstrap_brokers` output once that stack is actually
  applied.
- The same `spark.sql.adaptive.*` conf `dev-pyspark/`'s job needs (coalesce disabled, no
  broadcast, lowered skew threshold) travels with the job definition, not the script, so the
  script itself stays identical across both targets.

```sh
databricks jobs create --json @job.json   # not run in this session -- see "What this is, precisely"
```

## Status

IaC written, formatted, and locally validated. Not done: anything requiring a live AWS
account or Databricks workspace to confirm — an actual `terraform apply`, a real job run, and
wiring `store-postgres/`'s consumer to run as a long-lived service against the resulting MSK
cluster (ECS Fargate task definition, not written yet).

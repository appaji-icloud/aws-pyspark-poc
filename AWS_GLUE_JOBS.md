# AWS Glue layer jobs

The ETL is split into three independently runnable AWS Glue Spark jobs. Each
job receives the same batch `INPUT_DATE` and reads/writes the date-scoped S3
locations configured in `constants/aws_constants.py`.

| Job | Default job name | Script entry point | Reads | Writes |
|---|---|---|---|---|
| Raw to processed | `YouTubeRawToProcessed` | `glue_etl/jobs/raw_to_processed.py` | Raw CSV/JSON/JSONL and optional XLSX; requires `source/<date>/_READY` | Processed `events/` and `invalid/` |
| Processed to curated | `YouTubeProcessedToCurated` | `glue_etl/jobs/processed_to_curated.py` | Processed `events/` | Curated `main_video_events/` and `dim_user/`, `dim_video/`, `dim_channel/`, `dim_device/`, `dim_date/` |
| Curated fact | `YouTubeCuratedFact` | `glue_etl/jobs/curated_fact.py` | Curated main events and all five dimensions | Curated `fact_video_events/` |

The three paths in the **Script entry point** column are separate Glue script
files. Set one of those paths as each Glue job's Script path. Shared startup
code lives in `glue_etl/jobs/runner.py`; it is included in the dependency ZIP
and imported by each entry point, not configured as a script path.

## Configure each Glue job

Create three Glue Spark jobs in the same AWS region. For each job:

1. Upload the corresponding entry-point file from the table above to S3 and
   set that file as the Glue **Script path**.
2. Build a dependency ZIP with `common/`, `constants/`, `exceptions/`, and
   `glue_etl/` packages at the ZIP root. Include all subpackages, including
   `glue_etl/jobs/runner.py`; do not include `.venv`, tests, local credentials,
   or bundled PySpark.
3. Set `--extra-py-files` to that dependency ZIP.
4. Select the same Glue version and compatible worker configuration for all
   three jobs.
5. Assign a Glue execution role with read/list access to raw and processed
   inputs, read/write/list/delete permissions on processed and curated outputs
   (overwrite needs delete), and Glue/CloudWatch logging permissions. Grant
   SNS publish permissions only if SNS is configured.
6. For a direct/manual job run, supply `--INPUT_DATE` in `YYYY-MM-DD` format.
   Workflow-started jobs instead obtain `INPUT_DATE` and optional overrides
   from the workflow run properties. Other supported job arguments override
   the defaults in `AWSConstants`:

   | Argument | Default source |
   |---|---|
   | `--RAW_BUCKET` | `AWSConstants.RAW_BUCKET` |
   | `--PROCESSED_BUCKET` | `AWSConstants.PROCESSED_BUCKET` |
   | `--CURATED_BUCKET` | `AWSConstants.CURATED_BUCKET` |
   | `--AWS_REGION` | `AWSConstants.REGION` |
   | `--ENVIRONMENT` | `AWSConstants.DEFAULT_ENVIRONMENT` |
   | `--DATA_QUALITY_THRESHOLD` | `AWSConstants.DEFAULT_DATA_QUALITY_THRESHOLD` |
   | `--ENABLE_XLSX` | `AWSConstants.DEFAULT_ENABLE_XLSX` |
   | `--WRITE_MODE` | `AWSConstants.DEFAULT_WRITE_MODE` |
   | `--SNS_TOPIC_ARN` | `AWSConstants.SNS_TOPIC_ARN` |

The Glue job's actual name is used when Glue supplies `JOB_NAME`; otherwise,
the stage-specific default name is used.

## Run order

Run the jobs sequentially for the same date:

```text
raw-to-processed
        |
        v (only after SUCCEEDED)
processed-to-curated
        |
        v (only after SUCCEEDED)
curated-fact
```

For scheduled or event-driven execution, create an AWS Glue Workflow with an
on-demand start trigger for the raw-to-processed job and conditional triggers
that start each following job only when the preceding job succeeds. Set the
conditional triggers to depend on the preceding job's `SUCCEEDED` state. Each
job resolves `INPUT_DATE` and runtime overrides from the shared workflow run
properties, which take precedence over saved job defaults.

To start the complete workflow from the optional S3/EventBridge/Lambda
integration, set `GLUE_WORKFLOW_NAME` on Lambda to the workflow name. The
Lambda calls `StartWorkflowRun` with the batch `INPUT_DATE`; it does not start
a Glue job directly in this mode. Grant the Lambda role
`glue:StartWorkflowRun` on that workflow. For manual single-job starts, set
`--INPUT_DATE` directly on that Glue job. If Lambda is configured without
`GLUE_WORKFLOW_NAME`, it uses `GLUE_JOB_NAME` to start only that job; downstream
stages must then be started separately.

In workflow mode, set Lambda environment variables `RAW_BUCKET`,
`PROCESSED_BUCKET`, and `CURATED_BUCKET`, plus `GLUE_WORKFLOW_NAME`. Optional
overrides are `AWS_REGION`, `ENVIRONMENT`, `SNS_TOPIC_ARN`,
`DATA_QUALITY_THRESHOLD`, `ENABLE_XLSX`, and `WRITE_MODE`. In workflow mode,
`GLUE_WORKFLOW_NAME` selects the workflow; `GLUE_JOB_NAME` is not used.

The Glue job role needs `glue:GetWorkflowRunProperties` when jobs obtain
`INPUT_DATE` from workflow run properties. The producer must upload all raw
files and create `source/<date>/_READY` last.

### Deploy the S3-triggered Lambda

The Lambda entry point is
`glue_etl.lambda_orchestration.lambda_function.lambda_handler`. Build a ZIP
with `common/`, `constants/`, `exceptions/`, and `glue_etl/` at its root, then
create a Python 3.11 Lambda using that ZIP as its code package and set the
handler to the entry point above. The same package layout used for
`--extra-py-files` works; do not include `.venv`, tests, or PySpark.

Set these Lambda environment variables:

| Variable | Value |
|---|---|
| `RAW_BUCKET` | Raw input bucket that receives `source/<date>/_READY` |
| `PROCESSED_BUCKET` | Processed output bucket |
| `CURATED_BUCKET` | Curated output bucket |
| `GLUE_WORKFLOW_NAME` | Name of the workflow containing the three jobs |

Set optional overrides (`ENVIRONMENT`, `SNS_TOPIC_ARN`,
`DATA_QUALITY_THRESHOLD`, `ENABLE_XLSX`, `WRITE_MODE`) only when needed.
`AWS_REGION` is provided by the Lambda runtime; ensure the function is in the
same region as the Glue workflow.

Enable EventBridge notifications on the raw S3 bucket. Create an EventBridge
rule with this event pattern, replacing the bucket name:

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["your-raw-bucket"]
    },
    "object": {
      "key": [
        {
          "wildcard": "source/*/_READY"
        }
      ]
    }
  }
}
```

Set the rule's target to the Lambda and allow EventBridge to invoke it. Give
the Lambda execution role CloudWatch Logs permissions and
`glue:StartWorkflowRun` for the configured workflow. The handler validates
that the event is from `RAW_BUCKET` and that the key is exactly
`source/YYYY-MM-DD/_READY`, then starts one workflow run with the date and
configured overrides. The workflow's conditional triggers—not Lambda—start the
second and third Glue jobs after each preceding job succeeds.

## Reruns and recovery

Pass the same `INPUT_DATE` to each stage. If a stage fails, inspect its Glue
run and CloudWatch logs, then rerun that stage after correcting the issue.
Rerun downstream stages in sequence after an upstream stage's outputs change.
Processed events and fact output overwrite date-scoped partitions; invalid
records append. Each curated dimension is stored under the selected date's
curated prefix. A Glue workflow does not make the separate S3 writes
transactional, so inspect already-written outputs when recovering a partial
run.

XLSX ingestion is disabled by default. If enabling it, make `pandas` and
`openpyxl` available to the first job's Glue runtime using Glue-supported
additional modules; do not assume they are preinstalled.

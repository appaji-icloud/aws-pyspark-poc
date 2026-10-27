import json
import subprocess
import sys

REGION = "ap-south-2"
BUCKET = "scala-spark-2026-670322850061-ap-south-2-an"
PREFIX = "Glue_Scripts"
DEPS = "s3://" + BUCKET + "/" + PREFIX + "/glue_dependencies.zip"

jobs = [
    {
        "job_name": "yt_raw_to_processed",
        "script_location": "s3://" + BUCKET + "/" + PREFIX + "/yt_raw_to_processed.py"
    },
    {
        "job_name": "yt-processed_to_curated",
        "script_location": "s3://" + BUCKET + "/" + PREFIX + "/processed_to_curated.py"
    },
    {
        "job_name": "yt-ccurated_fact",
        "script_location": "s3://" + BUCKET + "/" + PREFIX + "/yt-ccurated_fact.py"
    }
]

for job in jobs:
    job_update = {
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": job["script_location"],
            "PythonVersion": "3"
        },
        "GlueVersion": "5.1",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "DefaultArguments": {
            "--job-language": "python",
            "--enable-metrics": "",
            "--enable-continuous-cloudwatch-log": "true",
            "--extra-py-files": DEPS
        }
    }
    result = subprocess.run(
        [
            "aws", "glue", "update-job",
            "--job-name", job["job_name"],
            "--region", REGION,
            "--job-update", json.dumps(job_update)
        ],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print("  SUCCESS: " + job["job_name"])
    else:
        print("  FAILED:  " + job["job_name"] + " - " + result.stderr)
        sys.exit(1)

print("All 3 Glue jobs updated successfully.")

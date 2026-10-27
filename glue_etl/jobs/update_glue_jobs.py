import json
import subprocess
import sys

REGION = "ap-south-2"
BUCKET = "scala-spark-2026-670322850061-ap-south-2-an"
PREFIX = "Glue_Scripts"
DEPS = f"s3://{BUCKET}/{PREFIX}/glue_dependencies.zip"

jobs = [
    {
        "job_name": "yt_raw_to_processed",
        "script_location": f"s3://{BUCKET}/{PREFIX}/yt_raw_to_processed.py"
    },
    {
        "job_name": "yt-processed_to_curated",
        "script_location": f"s3://{BUCKET}/{PREFIX}/processed_to_curated.py"
    },
    {
        "job_name": "yt-ccurated_fact",
        "script_location": f"s3://{BUCKET}/{PREFIX}/yt-ccurated_fact.py"
    }
]

for job in jobs:
 

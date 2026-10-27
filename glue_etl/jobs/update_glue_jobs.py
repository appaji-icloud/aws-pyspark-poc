version: 0.2

env:
  variables:
    REGION: "ap-south-2"
    BUCKET: "scala-spark-2026-670322850061-ap-south-2-an"
    PREFIX: "Glue_Scripts"

phases:
  install:
    runtime-versions:
      python: 3.11
    commands:
      - pip install boto3 --quiet

  pre_build:
    commands:
      - echo "Validating scripts"
      - python -m py_compile glue_etl/jobs/raw_to_processed.py
      - echo "OK - raw_to_processed.py"
      - python -m py_compile glue_etl/jobs/processed_to_curated.py
      - echo "OK - processed_to_curated.py"
      - python -m py_compile glue_etl/jobs/curated_fact.py
      - echo "OK - curated_fact.py"
      - echo "All scripts valid"

  build:
    commands:
      - echo "Uploading scripts to S3"
      - aws s3 cp glue_etl/jobs/raw_to_processed.py s3://${BUCKET}/${PREFIX}/yt_raw_to_processed.py --region ${REGION}
      - aws s3 cp glue_etl/jobs/processed_to_curated.py s3://${BUCKET}/${PREFIX}/processed_to_curated.py --region ${REGION}
      - aws s3 cp glue_etl/jobs/curated_fact.py s3://${BUCKET}/${PREFIX}/yt-ccurated_fact.py --region ${REGION}
      - aws s3 cp glue_dependencies.zip s3://${BUCKET}/${PREFIX}/glue_dependencies.zip --region ${REGION}
      - echo "Updating Glue jobs"
      - python3 glue_etl/jobs/update_glue_jobs.py

  post_build:
    commands:
      - echo "Deployment complete"

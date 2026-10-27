from constants.aws_constants import AWSConstants
from glue_etl.jobs.runner import run_glue_job
from glue_etl.pipeline_orchestrator import RawToProcessedOrchestrator


def main() -> None:
    run_glue_job(
        RawToProcessedOrchestrator,
        AWSConstants.RAW_TO_PROCESSED_JOB_NAME,
    )


if __name__ == "__main__":
    main()

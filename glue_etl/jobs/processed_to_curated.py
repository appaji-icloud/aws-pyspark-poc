from constants.aws_constants import AWSConstants
from glue_etl.jobs.runner import run_glue_job
from glue_etl.pipeline_orchestrator import ProcessedToCuratedOrchestrator


def main() -> None:
    run_glue_job(
        ProcessedToCuratedOrchestrator,
        AWSConstants.PROCESSED_TO_CURATED_JOB_NAME,
    )


if __name__ == "__main__":
    main()

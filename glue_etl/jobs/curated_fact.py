from constants.aws_constants import AWSConstants
from glue_etl.jobs.runner import run_glue_job
from glue_etl.pipeline_orchestrator import CuratedFactOrchestrator


def main() -> None:
    run_glue_job(
        CuratedFactOrchestrator,
        AWSConstants.CURATED_FACT_JOB_NAME,
    )


if __name__ == "__main__":
    main()

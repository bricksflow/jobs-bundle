from logging import Logger
from databricks_cli.jobs.api import JobsApi


class JobIdFinder:
    def __init__(
        self,
        logger: Logger,
        jobs_api: JobsApi,
    ):
        self.__logger = logger
        self.__jobs_api = jobs_api

    def find(self, job_name: str):
        jobs_response = self.__jobs_api.list_jobs()

        if "jobs" not in jobs_response:
            self.__logger.info("No jobs exist")
            return None

        jobs = jobs_response["jobs"]
        for job in jobs:
            if job["settings"]["name"] == job_name:
                return job["job_id"]
        return None

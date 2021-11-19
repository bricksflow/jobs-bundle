from argparse import ArgumentParser, Namespace
from logging import Logger
from box import Box
from consolebundle.ConsoleCommand import ConsoleCommand
from jobsbundle.job.ValuesFiller import ValuesFiller
from databricks_cli.jobs.api import JobsApi
from jobsbundle.job.JobPermissionUpdater import JobPermissionUpdater


class StreamingJobCreateCommand(ConsoleCommand):
    def __init__(
        self, jobs_raw_config: Box, logger: Logger, jobs_api: JobsApi, values_filler: ValuesFiller, permission_updater: JobPermissionUpdater
    ):
        self.__jobs_raw_config = jobs_raw_config
        self.__logger = logger
        self.__jobs_api = jobs_api
        self.__values_filler = values_filler
        self.__permission_updater = permission_updater

    def get_command(self) -> str:
        return "databricks:job:streaming-create"

    def get_description(self):
        return "Creates new Databricks streaming job based on given job identifier"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Job identifier")

    def run(self, input_args: Namespace):
        if input_args.identifier not in self.__jobs_raw_config:
            self.__logger.error(
                f"No job found for {input_args.identifier}. Maybe you forgot to add the configuration under jobsbundle.jobs?"
            )
            return

        job_raw_config = self.__jobs_raw_config[input_args.identifier].to_dict()
        values = job_raw_config["values"] if "values" in job_raw_config else {}
        job_config = self.__values_filler.fill(job_raw_config["template"], values, input_args.identifier)

        job_id = self.__jobs_api.create_job(job_config.to_dict())["job_id"]
        self.__logger.info(f"Job with ID {job_id} successfully created")

        self.__jobs_api.run_now(job_id)
        self.__logger.info(f"Job with ID {job_id} successfully run")

        if "permission" in job_raw_config:
            self.__permission_updater.run(job_raw_config["permission"], job_id)

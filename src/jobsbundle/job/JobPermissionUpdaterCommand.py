import sys
from argparse import ArgumentParser, Namespace
from logging import Logger
from box import Box
from consolebundle.ConsoleCommand import ConsoleCommand
from jobsbundle.job.ValuesFiller import ValuesFiller
from jobsbundle.job.JobIdFinder import JobIdFinder
from jobsbundle.job.JobPermissionUpdater import JobPermissionUpdater


class JobPermissionUpdaterCommand(ConsoleCommand):
    def __init__(
        self,
        jobs_raw_config: Box,
        logger: Logger,
        values_filler: ValuesFiller,
        job_id_finder: JobIdFinder,
        permission_updater: JobPermissionUpdater,
    ):
        self.__jobs_raw_config = jobs_raw_config
        self.__logger = logger
        self.__values_filler = values_filler
        self.__job_id_finder = job_id_finder
        self.__permission_updater = permission_updater

    def get_command(self) -> str:
        return "databricks:job:set-permissions"

    def get_description(self):
        return "Update permissions for an existing Databricks job based on given job identifier"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Job identifier")

    def run(self, input_args: Namespace):
        if input_args.identifier not in self.__jobs_raw_config:
            self.__logger.error(
                f"No job found for {input_args.identifier}. Maybe you forgot to add the configuration under jobsbundle.jobs?"
            )
            sys.exit(1)

        job_raw_config = self.__jobs_raw_config[input_args.identifier].to_dict()
        values = job_raw_config["values"] if "values" in job_raw_config else {}
        job_config = self.__values_filler.fill(job_raw_config["template"], values, input_args.identifier)
        self.__logger.info(f'Looking for job with name "{job_config.name}"')

        job_id = self.__job_id_finder.find(job_config.name)

        if not job_id:
            self.__logger.error(f'No existing job with name "{job_config.name}" found')
            sys.exit(1)

        if "permission" not in job_raw_config:
            self.__logger.error("No permissions set in config")
            sys.exit(1)

        self.__permission_updater.run(job_raw_config["permission"], job_id)

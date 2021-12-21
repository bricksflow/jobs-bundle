from argparse import Namespace, ArgumentParser
from logging import Logger
from box import Box
from consolebundle.ConsoleCommand import ConsoleCommand
from jobsbundle.job import argparser_configurator
from jobsbundle.job.JobCreateOrUpdateCommand import JobCreateOrUpdateCommand


class AllJobsCreatorCommand(ConsoleCommand):
    def __init__(
        self,
        jobs_raw_config: Box,
        logger: Logger,
        job_create_or_update_command: JobCreateOrUpdateCommand,
    ):
        self.__jobs_raw_config = jobs_raw_config or Box({})
        self.__logger = logger
        self.__job_create_or_update_command = job_create_or_update_command

    def get_command(self) -> str:
        return "dbx:job:create-all"

    def get_description(self):
        return "Create all Databricks jobs based on app configuration"

    def configure(self, argument_parser: ArgumentParser):
        argparser_configurator.add_kwargs(argument_parser)

    def run(self, input_args: Namespace):
        self.__logger.info(f"{len(self.__jobs_raw_config)} jobs to be created or updated")

        values = argparser_configurator.extract_kwargs(input_args)

        for identifier, _ in self.__jobs_raw_config.items():
            values["identifier"] = identifier
            input_args = Namespace(**values)

            self.__job_create_or_update_command.run(input_args)

        self.__logger.info("All jobs created or updated")

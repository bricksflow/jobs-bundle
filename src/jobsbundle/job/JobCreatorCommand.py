import sys
from argparse import ArgumentParser, Namespace
from logging import Logger
from box import Box
from consolebundle.ConsoleCommand import ConsoleCommand
from jobsbundle.job import argparser_configurator
from databricks_cli.jobs.api import JobsApi
from jobsbundle.job.JobIdFinder import JobIdFinder
from jobsbundle.job.Router import Router
from jobsbundle.job.ValuesFiller import ValuesFiller


class JobCreatorCommand(ConsoleCommand):
    def __init__(
        self,
        jobs_raw_config: Box,
        logger: Logger,
        jobs_api: JobsApi,
        job_id_finder: JobIdFinder,
        values_filler: ValuesFiller,
        router: Router,
    ):
        self.__jobs_raw_config = jobs_raw_config or Box({})
        self.__logger = logger
        self.__jobs_api = jobs_api
        self.__job_id_finder = job_id_finder
        self.__values_filler = values_filler
        self.__router = router

    def get_command(self) -> str:
        return "dbx:job:create"

    def get_description(self):
        return "Create a new Databricks job based on given job identifier"

    def configure(self, argument_parser: ArgumentParser):
        argument_parser.add_argument(dest="identifier", help="Job identifier")
        argparser_configurator.add_kwargs(argument_parser)

    def run(self, input_args: Namespace):
        if input_args.identifier not in self.__jobs_raw_config:
            self.__logger.error(
                f"No job found for {input_args.identifier}. Maybe you forgot to add the configuration under jobsbundle.jobs?"
            )
            sys.exit(1)

        job_raw_config = self.__jobs_raw_config[input_args.identifier].to_dict()
        values = argparser_configurator.extract_kwargs(input_args)
        job_config = self.__values_filler.fill(job_raw_config, values, input_args.identifier)

        job_id = self.__job_id_finder.find(job_config.name)

        if job_id:
            job_url = self.__router.generate_url("job.detail", job_id=job_id)
            self.__logger.error(f'Job with name "{job_config.name}" already exist, exiting {job_url}')
            sys.exit(1)

        self.__logger.info(f'Creating job {input_args.identifier} with name "{job_config.name}"')

        self.__jobs_api.create_job(job_config)

        job_url = self.__router.generate_url("job.detail", job_id=job_id)
        self.__logger.info(f"Job successfully created {job_url}")

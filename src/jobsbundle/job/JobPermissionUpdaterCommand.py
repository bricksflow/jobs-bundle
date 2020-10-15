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
        jobsRawConfig: Box,
        logger: Logger,
        valuesFiller: ValuesFiller,
        jobIdFinder: JobIdFinder,
        permissionUpdater: JobPermissionUpdater
    ):
        self.__jobsRawConfig = jobsRawConfig
        self.__logger = logger
        self.__valuesFiller = valuesFiller
        self.__jobIdFinder = jobIdFinder
        self.__permissionUpdater = permissionUpdater

    def getCommand(self) -> str:
        return 'databricks:job:set-permissions'

    def getDescription(self):
        return 'Update permissions for an existing Databricks job based on given job identifier'

    def configure(self, argumentParser: ArgumentParser):
        argumentParser.add_argument(dest='identifier', help='Job identifier')

    def run(self, inputArgs: Namespace):
        if inputArgs.identifier not in self.__jobsRawConfig:
            self.__logger.error('No job found for {}. Maybe you forgot to add the configuration under jobsbundle.jobs?'.format(inputArgs.identifier))
            sys.exit(1)

        jobRawConfig = self.__jobsRawConfig[inputArgs.identifier].to_dict()
        jobConfig = self.__valuesFiller.fill(
            jobRawConfig['template'],
            jobRawConfig['values'],
            inputArgs.identifier
        )
        self.__logger.info(f'Looking for job with name "{jobConfig.name}"')

        jobId = self.__jobIdFinder.find(jobConfig.name)

        if not jobId:
            self.__logger.error(f'No existing job with name "{jobConfig.name}" found')
            sys.exit(1)

        if 'permission' not in jobRawConfig:
            self.__logger.error(f'No permissions set in config')
            sys.exit(1)

        self.__permissionUpdater.run(jobRawConfig['permission'], jobId)

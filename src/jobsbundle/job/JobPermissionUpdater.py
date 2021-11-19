import sys
from logging import Logger
import requests
import json


class JobPermissionUpdater:
    def __init__(
        self,
        logger: Logger,
        databricks_host: str,
        databricks_token: str,
    ):
        self.__logger = logger
        self.__host = databricks_host
        self.__token = databricks_token

    def run(self, config_permission, job_id):
        res_of_change = self.__change_permissions(config_permission, job_id)

        if res_of_change.status_code != 200:
            self.__logger.error(f"Permissions for job ID {job_id} were not changed")
            self.__logger.error(res_of_change.text)
            sys.exit(1)

        self.__logger.info(f"Permissions for job ID {job_id} updated")
        res_of_check = self.__check_permissions(job_id)
        self.__logger.info(f"Current permissions: {res_of_check.text}")

    def __change_permissions(self, config_permission, job_id):
        self.__logger.info(f"Changing permissions for job ID {job_id}")
        url = self.__create_url(job_id)
        auth = self.__create_auth()

        data = {
            "object_id": f"/jobs/{job_id}",
            "object_type": "job",
            "access_control_list": self.__create_access_control_list(config_permission),
        }

        return requests.patch(url, data=json.dumps(data), headers=auth)

    def __create_access_control_list(self, config_permission):
        if "users_names" in config_permission:
            return [
                {"user_name": user_name, "permission_level": config_permission["permission_level"]}
                for user_name in config_permission["users_names"]
            ]

        if "groups" in config_permission:
            return [
                {"group_name": group_name, "permission_level": config_permission["permission_level"]}
                for group_name in config_permission["groups"]
            ]

        raise Exception(f"Invalid permissions: {config_permission}")

    def __check_permissions(self, job_id):
        self.__logger.info("Checking the results after update")
        url = self.__create_url(job_id)
        auth = self.__create_auth()
        return requests.get(url, headers=auth)

    def __create_url(self, job_id):
        return f"{self.__host}/api/2.0/preview/permissions/jobs/{job_id}"

    def __create_auth(self):
        return {"Authorization": f"Bearer {self.__token}"}

from box import Box
from jobsbundle.git.CurrentBranchResolver import CurrentBranchResolver
from jobsbundle.job.template_filler import fill_template


class ValuesFiller:
    def __init__(
        self,
        current_branch_resolver: CurrentBranchResolver,
    ):
        self.__current_branch_resolver = current_branch_resolver

    def fill(self, template: dict, values: dict, identifier: str) -> Box:
        values["identifier"] = identifier
        values["current_branch"] = self.__current_branch_resolver.resolve()

        def fill_dict_template(value):
            if isinstance(value, dict):
                return {k: fill_dict_template(v) for k, v in value.items()}

            if isinstance(value, list):
                return list(map(fill_dict_template, value))

            if isinstance(value, str):
                return fill_template(value, values)

            return value

        return Box({k: fill_dict_template(v) for k, v in template.items()})

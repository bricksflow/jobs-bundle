import os
from pygit2 import Repository, discover_repository, GitError


class CurrentRepositoryFactory:
    def create(self):
        base_path = os.getcwd()
        repository_path = discover_repository(base_path)

        if not repository_path:
            raise GitError(f'No repository found at "{base_path}" and its parents')

        return Repository(repository_path)

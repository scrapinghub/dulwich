import shutil
from os import listdir
from os.path import join, isdir

from .repo import Repo


class FsRepo(Repo):

    @classmethod
    def setup(cls, location):
        cls.base_dir = location

    @classmethod
    def init_bare(cls, name):
        return Repo.init(join(cls.base_dir, name), mkdir=True)

    @classmethod
    def open(cls, name):
        """Open an existing repository.
        """
        return cls(join(cls.base_dir, name))

    @classmethod
    def repo_exists(cls, name):
        """Checks if a repository exists.
        """
        return isdir(join(cls.base_dir, name))        
        
    @classmethod
    def list_repos(cls):
        """List all repository names.
        """
        try:
            for fname in listdir(cls.base_dir):
                if isdir(join(cls.base_dir, fname)):
                    yield fname
        except OSError as ex:
            if ex.errno != errno.ENOENT:
                raise   

    @classmethod
    def delete_repo(cls, name):
        """Deletes a repository.
        """
        shutil.rmtree(join(cls.base_dir, name))

from git import Repo
from os import getenv, path, chmod
import shutil
from stat import S_IWRITE
from warnings import warn
from psycopg2 import connect
from json import load
from enum import Enum
import sys
from maskpass import askpass


# pyinstaller postgres_builder.py --distpath '%userprofile%/Desktop/atata' --clean --workpath
# '%userprofile%/Desktop/atata/build' --add-data "default_properties.json:." --add-data "misc:misc" --onefile


class PostgresObjInstaller:

    @staticmethod
    def resource_path(relative_path):
        """ Get absolute path to resource, works for dev and for PyInstaller """
        base_path = getattr(sys, '_MEIPASS', path.dirname(path.abspath(__file__)))
        return path.join(base_path, relative_path)

    def __init__(self):
        with open(self.resource_path(r'default_properties.json'), mode='rt',
                  encoding='UTF-8') as f:
            data = load(f)
        self.repo_properties = self.RepositoryProperties(data['repo'])
        self.db_properties = self.PGConnectionProperties(data['db'])
        self.repo = None
        self.script_list = None
        self.deploy_mode = self.DeployMode.SEPARATE_STATEMENTS.value

    def __setattr__(self, key, value):
        if (key in self.__dict__ and value != '') or key not in self.__dict__:
            object.__setattr__(self, key, value)

    class RepositoryProperties:
        def __init__(self, properties_dict):
            for k, v in properties_dict.items():
                self.__setattr__(k, v)

        def __repr__(self):

            t = ', '.join(f'{k}={v}' for k, v in self.__dict__.items())
            return f'{self.__class__.__name__}({t})'

        def __setattr__(self, key, value):
            def decode_init_local_path(local_path: dict):
                if local_path['env'] is None:
                    return path.abspath(local_path['path'])
                return path.abspath(getenv(local_path['env']) + local_path['path'])

            if isinstance(value, dict):
                value = decode_init_local_path(value)
            if (key in self.__dict__ and value != '') or key not in self.__dict__:
                object.__setattr__(self, key, value)

    class PGConnectionProperties:
        def __init__(self, properties_dict):
            for k, v in properties_dict.items():
                object.__setattr__(self, k, v)

        def __repr__(self):
            t = ', '.join(f'{k}={v}' for k, v in self.__dict__.items())
            return f'{self.__class__.__name__}({t})'

        def __setattr__(self, key, value):
            if (key in self.__dict__ and value != '') or key not in self.__dict__:
                object.__setattr__(self, key, value)

        def as_dict(self):
            return {k: v for k, v in self.__dict__.items() if not callable(v)}

    def clone_repo(self):

        self.repo_properties.remote_path = input(
            f'Enter remote repo path, default is: {self.repo_properties.remote_path}\n').strip()
        self.repo_properties.local_path = input(
            f'Enter the local path where repo will be cloned, default is: {self.repo_properties.local_path}\n').strip()

        def remove_readonly(func, fpath, *args):
            chmod(fpath, S_IWRITE)
            func(fpath)

        try:
            warn(f'Folder will be overwritten: {self.repo_properties.local_path}', category=RuntimeWarning,
                 stacklevel=2)
            shutil.rmtree(self.repo_properties.local_path, onerror=remove_readonly)
        except FileNotFoundError:
            pass

        self.repo = Repo.clone_from(self.repo_properties.remote_path, self.repo_properties.local_path)

    def switch_branch(self):
        self.repo_properties.branch = input(
            f'Enter a branch name or commit SHA-1, default branch is: {self.repo_properties.branch}\n').strip()
        print(f'Switching repo to {self.repo_properties.branch}')
        self.repo.git.checkout(self.repo_properties.branch)

    @staticmethod
    def check_scripts(script_list: list[tuple]):
        for _, __ in script_list:
            if not path.exists(_):
                print(f'Specified script doesn\'t exist {__}')
                exit('Fill objects.inst file with correct script paths and try again')
        return script_list

    def check_folder_and_scripts(self):
        self.repo_properties.folder = input(
            f'Enter a subfolder name of Requests catalog(must contain objects.inst file)'
            f', default folder is: {self.repo_properties.folder}\n').strip()

        inst_path = path.abspath(
            fr'{self.repo_properties.local_path}/Requests/{self.repo_properties.folder}/objects.inst')

        with open(inst_path, mode='rt', encoding='UTF-8') as f:
            file_paths = [(path.abspath(fr'{self.repo_properties.local_path}/{_.rstrip()}'), _.rstrip()) for _ in f if
                          not _.startswith('#')]
        self.script_list = self.check_scripts(file_paths)
        print(f'List of deploy scripts created successfully')

    @staticmethod
    def read_sql(filepath):
        with open(filepath, mode='rt', encoding='UTF-8') as f:
            sql = f.read()
        return sql

    def check_connection(self):
        self.db_properties.host = input(
            f'Enter the host of the Postgresql cluster, default host is: {self.db_properties.host}\n')
        self.db_properties.port = input(
            f'Enter the port of the Postgresql cluster, default port is: {self.db_properties.port}\n')
        self.db_properties.dbname = input(
            f'Enter the database name, default database is: {self.db_properties.dbname}\n')
        self.db_properties.user = input(
            f'Enter the user name for db connection, default user is: {self.db_properties.user}\n')
        self.db_properties.password = askpass(prompt=f'Enter the password for db connection\n')
        print(self.db_properties.password)
        connection = connect(**self.db_properties.as_dict())
        connection.set_session(autocommit=True)

        return connection

    @staticmethod
    def execute_script(sql, connection):
        with connection.cursor() as cc:
            cc.execute(sql)
            print(cc.statusmessage)

    class DeployMode(Enum):
        SEPARATE_STATEMENTS = 'separate'
        SINGLE_STATEMENT = 'single'

    def deploy_objects(self):
        self.deploy_mode = input(
            f'Execute scripts as single statement or separately (single/separate)? '
            f'Default mode is: {self.deploy_mode}\n')
        if self.deploy_mode not in (_.value for _ in self.DeployMode):
            raise RuntimeError('Invalid deploy mode')

        connection = self.check_connection()

        if self.deploy_mode == self.DeployMode.SEPARATE_STATEMENTS.value:

            for _, __ in self.script_list:
                try:
                    print(f'Executing script: {__}')
                    self.execute_script(self.read_sql(_), connection)
                except Exception as e:
                    print(e)
                    exit('Got errors during deploy execution, further execution is stopped')
        else:
            fname = r'cur_install.sql'
            fpath = path.abspath(f'{self.repo_properties.local_path}/{fname}')

            with open(fpath, mode='wt', encoding='UTF-8') as f1:
                with open(self.resource_path(r'misc/start_single_statement.txt'), mode='rt', encoding='UTF-8') as f2:
                    st = f2.read()
                f1.write(st)

                for _, __ in self.script_list:
                    f1.write(f'{self.read_sql(_)}\n\n')

                with open(self.resource_path(r'misc/end_single_statement.txt'), mode='rt', encoding='UTF-8') as f2:
                    st = f2.read()

                f1.write(st)
            try:
                print(f'Executing script: {fpath}')
                self.execute_script(self.read_sql(fpath), connection)
            except Exception as e:
                print(e)
                exit('Got errors during deploy execution, further execution is stopped')


if __name__ == '__main__':
    try:
        pg_builder = PostgresObjInstaller()
        pg_builder.clone_repo()
        pg_builder.switch_branch()
        pg_builder.check_folder_and_scripts()
        pg_builder.deploy_objects()
    except Exception:

        print(sys.exc_info()[0])
        from traceback import format_exc

        print(format_exc())
    finally:
        print('Press Enter to close the window')
        input()

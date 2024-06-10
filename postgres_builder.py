from os import getenv, path, chmod, environ, makedirs
from poi_lib import resource_path

environ['GIT_PYTHON_GIT_EXECUTABLE'] = path.abspath(resource_path(r'misc/PortableGit-2.45.0-64-bit/bin/git.exe'))

from git import Repo
import shutil
from stat import S_IWRITE
from psycopg2 import connect
from json import load
from enum import Enum
import sys
from maskpass import askpass
from datetime import datetime
from termcolor import colored, cprint
from colorama import just_fix_windows_console
from collections import namedtuple


class PostgresObjInstaller:
    __properties_file = r'default_properties.json'
    __log_file = r'install.log'
    __prompts_default = ['cyan', None, ['bold']]
    __encoding = r'UTF-8'
    __inst_file = r'objects.inst'
    __revert_file = r'objects.revert'

    def __deploy_type_file_map(self, deploy_type):
        if deploy_type == self.DeployType.RELEASE.value:
            return self.__inst_file
        if deploy_type == self.DeployType.REVERT.value:
            return self.__revert_file
        raise ValueError('Invalid deploy type')

    class DeployType(Enum):
        RELEASE = 'release'
        REVERT = 'revert'

    def log_and_print(self, message, color, attrs=None):
        with open(f'{self.repo_properties.dist_path}/{self.__dist_folder_name}/{self.__log_file}', mode='a',
                  encoding=self.__encoding) as f:
            f.write(f'{datetime.now()}: {message}\n')

        cprint(message, color=color, attrs=attrs)

    def __init__(self):
        with open(resource_path(self.__properties_file), mode='rt',
                  encoding=self.__encoding) as f:
            data = load(f)

        self.repo_properties = self.RepositoryProperties(data['repo'])
        self.db_properties = self.PGConnectionProperties(data['db']['connection'])
        self.repo_properties.revert_branch = None
        self.repo = None
        self.script_list = None
        self.deploy_mode = self.DeployMode.SEPARATE_STATEMENTS.value
        self.deploy_type = self.DeployType.RELEASE.value
        self.log_table = data['db']['log_table']
        self.__dist_folder_name = None
        self.__release_branch = None
        self.__revert_branch = None

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

        cprint(f'Enter the remote repo path, default is: {self.repo_properties.remote_path}', *self.__prompts_default)
        self.repo_properties.remote_path = input().strip()

        cprint(f'Remote repo path is set to {self.repo_properties.remote_path}', 'light_green')

        cprint(f'Enter the local path where repo will be cloned, default is: {self.repo_properties.local_path}',
               *self.__prompts_default)
        self.repo_properties.local_path = input().strip()

        cprint(f'Local repo path is set to {self.repo_properties.local_path}', 'light_green')

        def remove_readonly(func, fpath, *args):
            chmod(fpath, S_IWRITE)
            func(fpath)

        try:

            cprint(f'Folder will be overwritten: {self.repo_properties.local_path}', 'red', attrs=['bold'])
            shutil.rmtree(self.repo_properties.local_path, onerror=remove_readonly)
        except FileNotFoundError:
            pass

        cprint('Cloning repository...', 'yellow')
        self.repo = Repo.clone_from(self.repo_properties.remote_path, self.repo_properties.local_path)
        cprint('Repository cloned successfully', 'light_green', attrs=['bold'])

    def handle_deploy_path(self):
        cprint(f'Select deploy type (release/revert) '
               f'Default type is: {self.deploy_type}', color='cyan', attrs=['bold'])
        self.deploy_type = input().strip()
        cprint(f'Deploy type is set to {self.deploy_type}', 'light_green')

        if self.deploy_type not in (_.value for _ in self.DeployType):
            raise RuntimeError(colored('Invalid deploy type', 'red', attrs=['bold']))

        if self.deploy_type == self.DeployType.RELEASE.value:
            self.switch_to_release_branch()
            self.create_dist_folder()
            self.check_folder_and_scripts()
            self.copy_scripts_to_dist_path()
        else:  # self.deploy_type == self.DeployType.REVERT.value
            self.switch_to_release_branch()
            self.create_dist_folder()
            self.check_folder_and_scripts()
            self.copy_scripts_to_dist_path(self.RevertStage.ONE.value)
            self.switch_to_revert_branch()
            self.copy_scripts_to_dist_path(self.RevertStage.TWO.value)

    def create_dist_folder(self):
        _format = '%Y-%d-%m %H.%M.%S'
        self.get_dist_folder_name = f'{self.deploy_type} {self.get_branch()} {datetime.now().strftime(_format)}'
        makedirs(path.abspath(
            fr'{self.repo_properties.dist_path}/{self.get_dist_folder_name}'))

    def switch_to_release_branch(self):

        cprint(f'Enter a release branch name or commit SHA-1, default branch is: {self.repo_properties.release_branch}',
               *self.__prompts_default)
        self.repo_properties.release_branch = input().strip()
        cprint(f'Release ranch/SHA-1 is set to {self.repo_properties.release_branch}', 'light_green')
        cprint('Checking out...', 'yellow')
        self.repo.git.checkout(self.repo_properties.release_branch)
        cprint('Checkout is successful', 'light_green')
        self.__release_branch = self.get_branch()

    def switch_to_revert_branch(self):

        cprint(f'Enter a revert branch name or commit SHA-1, default branch is: {self.repo_properties.revert_branch}',
               *self.__prompts_default)
        self.repo_properties.revert_branch = input().strip()
        cprint(f'Revert branch/SHA-1 is set to {self.repo_properties.revert_branch}', 'light_green')
        cprint('Checking out...', 'yellow')
        self.repo.git.checkout(self.repo_properties.revert_branch)
        cprint('Checkout is successful', 'light_green')
        self.__revert_branch = self.get_branch()

    def check_scripts(self, script_list: list[tuple]):
        for _, __, k in script_list:
            if not path.exists(_):
                self.log_and_print(f'Specified script doesn\'t exist {__}', 'red')
                self.log_and_print('Fill objects.inst file with correct script paths and try again', 'red')
                sys.exit()
        return script_list

    def check_folder_and_scripts(self):

        cprint(
            f'Enter a subfolder name of Requests catalog(must contain {self.__deploy_type_file_map(self.deploy_type)} file)'
            f', default folder is: {self.repo_properties.folder}', *self.__prompts_default)
        self.repo_properties.folder = input().strip()

        cprint(f'Folder is set to {self.repo_properties.folder}', 'light_green')

        inst_path = path.abspath(
            fr'{self.repo_properties.local_path}/Requests/{self.repo_properties.folder}/{self.__deploy_type_file_map(self.deploy_type)}')

        Script = namedtuple('Script', ['repo_fpath', 'content_fpath', 'dist_fpath'])

        with open(inst_path, mode='rt', encoding=self.__encoding) as f:
            file_paths = [Script(path.abspath(fr'{self.repo_properties.local_path}/{_.rstrip()}')
                                 , _.rstrip()
                                 , path.abspath(
                    fr'{self.repo_properties.dist_path}/{self.get_dist_folder_name}/{_.rstrip()}'))
                          for _ in f if not _.startswith('#')]

        self.script_list = self.check_scripts(file_paths)
        self.log_and_print(f'List of deploy scripts created successfully', 'light_green')

    class RevertStage(Enum):
        ZERO = 0
        ONE = 1
        TWO = 2

    def copy_scripts_to_dist_path(self, revert_stage=RevertStage.ZERO.value):

        if revert_stage == self.RevertStage.ZERO.value:
            cprint('Copying scripts to dist path...', 'yellow')
            for a, l, d in self.script_list:
                makedirs(path.dirname(d), exist_ok=True)
                shutil.copy(a, d)
            else:
                cprint('Scripts copied successfully', 'light_green')
                cprint(fr'Deployment scripts location is {self.repo_properties.dist_path}\{self.get_dist_folder_name}',
                       'light_magenta')

        elif revert_stage == self.RevertStage.ONE.value:
            b = 0
            for i, t in enumerate(_ for _ in self.script_list if _.content_fpath.startswith('Requests')):
                if i == 0:
                    cprint('Copying first stage scripts to dist path...', 'yellow')
                makedirs(path.dirname(t.dist_fpath), exist_ok=True)
                shutil.copy(t.repo_fpath, t.dist_fpath)
                b += 1
            else:
                if b > 0:
                    cprint('First stage scripts copied successfully', 'light_green')
        else:  # revert_stage==self.RevertStage.TWO.valuen
            for i, t in enumerate(_ for _ in self.script_list if _.content_fpath.startswith('OBJ')):
                if i == 0:
                    cprint('Copying second stage scripts to dist path...', 'yellow')
                makedirs(path.dirname(t.dist_fpath), exist_ok=True)
                shutil.copy(t.repo_fpath, t.dist_fpath)
            else:
                cprint('Scripts copied successfully', 'light_green')
                cprint(fr'Deployment scripts location is {self.repo_properties.dist_path}\{self.get_dist_folder_name}',
                       'light_magenta')

    def read_sql(self, filepath):
        with open(filepath, mode='rt', encoding=self.__encoding) as f:
            sql = f.read()
        return sql

    def check_connection(self):
        cprint(f'Enter the host of the Postgresql cluster, default host is: {self.db_properties.host}',
               *self.__prompts_default)
        self.db_properties.host = input()
        cprint(f'Host is set to {self.db_properties.host}', 'light_green')

        cprint(f'Enter the port of the Postgresql cluster, default port is: {self.db_properties.port}',
               *self.__prompts_default)
        self.db_properties.port = input()
        cprint(f'Port is set to {self.db_properties.port}', 'light_green')

        cprint(f'Enter the database name, default database is: {self.db_properties.dbname}',
               *self.__prompts_default)
        self.db_properties.dbname = input()
        cprint(f'Database name is set to {self.db_properties.dbname}', 'light_green')

        cprint(f'Enter the user name for db connection, default user is: {self.db_properties.user}',
               *self.__prompts_default)
        self.db_properties.user = input()
        cprint(f'User is set to {self.db_properties.user}', 'light_green')

        self.db_properties.password = askpass(prompt=colored(f'Enter the password for db connection\n', 'blue'))
        connection = connect(**self.db_properties.as_dict())
        connection.set_session(autocommit=True)

        return connection

    def execute_script(self, sql, connection):
        with connection.cursor() as cc:
            cc.execute(sql)
            self.log_and_print(cc.statusmessage, 'magenta')

    class DeployMode(Enum):
        SEPARATE_STATEMENTS = 'separate'
        SINGLE_STATEMENT = 'single'

    def get_branch(self):
        try:
            return f'{self.repo.active_branch} {self.repo.head.commit}'
        except TypeError:
            return self.repo.head.commit

    def get_log_dml(self, is_successful):

        if self.deploy_type == self.DeployType.RELEASE.value:
            branch = repr(self.__release_branch)

        else:  # self.deploy_type==self.DeployType.REVERT.value:
            branch = repr(f'{self.__release_branch} to {self.__revert_branch}')

        return f'insert into {self.log_table}(branch, deploy_mode, is_successful, deploy_type, requests_folder) ' \
               f'values({branch}, {repr(self.deploy_mode)}, {is_successful}' \
               f', {repr(self.deploy_type)}, {repr(self.repo_properties.folder)})'

    @property
    def get_dist_folder_name(self):

        return self.__dist_folder_name

    @get_dist_folder_name.setter
    def get_dist_folder_name(self, value):
        if self.__dist_folder_name is None:
            self.__dist_folder_name = value

    def deploy_objects(self):
        cprint(f'Execute scripts as single statement or separately (single/separate)? '
               f'Default mode is: {self.deploy_mode}', color='cyan', attrs=['bold'])
        self.deploy_mode = input().strip()
        cprint(f'Deploy mode is set to {self.deploy_mode}', 'light_green')

        if self.deploy_mode not in (_.value for _ in self.DeployMode):
            raise RuntimeError(colored('Invalid deploy mode', 'red', attrs=['bold']))

        connection = self.check_connection()

        if self.deploy_mode == self.DeployMode.SEPARATE_STATEMENTS.value:

            for _, __, k in self.script_list:
                try:
                    self.log_and_print(f'Executing script: {__}', 'yellow')
                    self.execute_script(self.read_sql(k), connection)
                except Exception as e:
                    cprint(f'Logging ci info...', 'yellow')
                    self.execute_script(self.get_log_dml(False), connection)
                    self.log_and_print(e, 'red')
                    self.log_and_print('Got errors during deploy execution, further execution is stopped', 'red')
                    sys.exit()
            else:
                cprint(f'Logging ci info...', 'yellow')
                self.execute_script(self.get_log_dml(True), connection)

        else:
            fname = r'cur_install.sql'
            fpath = path.abspath(f'{self.repo_properties.dist_path}/{self.get_dist_folder_name}/{fname}')

            with open(fpath, mode='wt', encoding=self.__encoding) as f1:
                with open(resource_path(r'misc/start_single_statement.txt'), mode='rt', encoding=self.__encoding) as f2:
                    st = f2.read()
                f1.write(st)

                for _, __, k in self.script_list:
                    f1.write(f'{self.read_sql(k)}\n\n')

                with open(resource_path(r'misc/end_single_statement.txt'), mode='rt', encoding=self.__encoding) as f2:
                    st = f2.read()

                f1.write(st)
            try:
                self.log_and_print(f'Executing script: {fpath}', 'yellow')
                self.execute_script(self.read_sql(fpath), connection)
                cprint(f'Logging ci info...', 'yellow')
                self.execute_script(self.get_log_dml(True), connection)
            except Exception as e:
                cprint(f'Logging ci info...', 'yellow')
                self.execute_script(self.get_log_dml(False), connection)
                self.log_and_print(e, 'red')
                self.log_and_print('Got errors during deploy execution, further execution is stopped', 'red')
                sys.exit()


if __name__ == '__main__':
    just_fix_windows_console()
    try:
        pg_builder = PostgresObjInstaller()
        pg_builder.clone_repo()
        pg_builder.handle_deploy_path()
        pg_builder.deploy_objects()
    except Exception:
        print(colored(sys.exc_info()[0], 'red'))
        from traceback import format_exc

        print(colored(format_exc(), 'red'))
    finally:
        cprint('Press Enter to close the window', 'light_red')
        input()

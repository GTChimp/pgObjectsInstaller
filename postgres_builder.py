from os import getenv, path, chmod, environ, makedirs
from poi_lib import resource_path

environ['GIT_PYTHON_GIT_EXECUTABLE'] = path.abspath(resource_path(r'misc/PortableGit-2.45.0-64-bit/bin/git.exe'))

from git import Repo
import shutil
from stat import S_IWRITE
from warnings import warn
from psycopg2 import connect
from json import load
from enum import Enum
import sys
from maskpass import askpass
from datetime import datetime
from termcolor import colored, cprint
from colorama import just_fix_windows_console


class PostgresObjInstaller:
    __properties_file = r'default_properties.json'
    __log_file = r'install.log'
    __prompts_default=['cyan',None,['bold']]

    def log_and_print(self, message,color,attrs=None):
        with open(f'{self.repo_properties.dist_path}/{self.__dist_folder_name}/{self.__log_file}', mode='a', encoding='UTF-8') as f:
            f.write(f'{datetime.now()}: {message}\n')

        cprint(message,color=color,attrs=attrs)

    def __init__(self):
        with open(resource_path(self.__properties_file), mode='rt',
                  encoding='UTF-8') as f:
            data = load(f)

        self.repo_properties = self.RepositoryProperties(data['repo'])
        self.db_properties = self.PGConnectionProperties(data['db']['connection'])
        self.repo = None
        self.script_list = None
        self.deploy_mode = self.DeployMode.SEPARATE_STATEMENTS.value
        self.log_table = data['db']['log_table']
        self.__dist_folder_name = None



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

        cprint(f'Enter the local path where repo will be cloned, default is: {self.repo_properties.local_path}',*self.__prompts_default)
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

        cprint('Cloning repository...','yellow')
        self.repo = Repo.clone_from(self.repo_properties.remote_path, self.repo_properties.local_path)
        cprint('Repository cloned successfully', 'light_green',attrs=['bold'])

    def switch_branch(self):

        cprint(f'Enter a branch name or commit SHA-1, default branch is: {self.repo_properties.branch}', *self.__prompts_default)
        self.repo_properties.branch = input().strip()
        cprint(f'Branch/SHA-1 is set to {self.repo_properties.branch}', 'light_green')
        cprint('Checking out...','yellow')
        self.repo.git.checkout(self.repo_properties.branch)
        cprint('Checkout is successful', 'light_green')
        _format = '%Y-%d-%m %H.%M.%S'
        self.get_dist_folder_name = f'{self.get_branch()} {datetime.now().strftime(_format)}'
        makedirs(path.abspath(
            fr'{self.repo_properties.dist_path}/{self.get_dist_folder_name}'))

    def check_scripts(self, script_list: list[tuple]):
        for _, __, k in script_list:
            if not path.exists(_):
                self.log_and_print(f'Specified script doesn\'t exist {__}','red')
                self.log_and_print('Fill objects.inst file with correct script paths and try again','red')
                sys.exit()
        return script_list

    def check_folder_and_scripts(self):

        cprint(f'Enter a subfolder name of Requests catalog(must contain objects.inst file)'
               f', default folder is: {self.repo_properties.folder}',*self.__prompts_default)
        self.repo_properties.folder = input().strip()

        cprint(f'Folder is set to {self.repo_properties.folder}', 'light_green')

        inst_path = path.abspath(
            fr'{self.repo_properties.local_path}/Requests/{self.repo_properties.folder}/objects.inst')

        with open(inst_path, mode='rt', encoding='UTF-8') as f:
            file_paths = [(path.abspath(fr'{self.repo_properties.local_path}/{_.rstrip()}')
                           , _.rstrip()
                           , path.abspath(
                fr'{self.repo_properties.dist_path}/{self.get_dist_folder_name}/{_.rstrip()}'))
                          for _ in f if not _.startswith('#')]

        self.script_list = self.check_scripts(file_paths)
        self.log_and_print(f'List of deploy scripts created successfully','light_green')

    def copy_scripts_to_dist_path(self):
        cprint('Copying scripts to dist path...','yellow')
        for a, l, d in self.script_list:
            makedirs(path.dirname(d), exist_ok=True)
            shutil.copy(a, d)
        cprint('Scripts copied successfully', 'light_green')
        cprint(fr'Deployment scripts location is {self.repo_properties.dist_path}\{self.get_dist_folder_name}',
               'light_magenta')

    @staticmethod
    def read_sql(filepath):
        with open(filepath, mode='rt', encoding='UTF-8') as f:
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

        self.db_properties.password = askpass(prompt=colored(f'Enter the password for db connection\n','blue'))
        connection = connect(**self.db_properties.as_dict())
        connection.set_session(autocommit=True)

        return connection

    def execute_script(self, sql, connection):
        with connection.cursor() as cc:
            cc.execute(sql)
            self.log_and_print(cc.statusmessage,'magenta')

    class DeployMode(Enum):
        SEPARATE_STATEMENTS = 'separate'
        SINGLE_STATEMENT = 'single'

    def get_branch(self):
        try:
            return f'{self.repo.active_branch} {self.repo.head.commit}'
        except TypeError:
            return self.repo.head.commit

    def get_log_dml(self, is_successful):
        return f'insert into {self.log_table} values({repr(self.get_branch())}, {repr(self.deploy_mode)}, {is_successful})'

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
            raise RuntimeError(colored('Invalid deploy mode','red',attrs=['bold']))

        connection = self.check_connection()

        if self.deploy_mode == self.DeployMode.SEPARATE_STATEMENTS.value:

            for _, __, k in self.script_list:
                try:
                    self.log_and_print(f'Executing script: {__}','yellow')
                    self.execute_script(self.read_sql(k), connection)
                except Exception as e:
                    self.execute_script(self.get_log_dml(False), connection)
                    self.log_and_print(e,'red')
                    self.log_and_print('Got errors during deploy execution, further execution is stopped','red')
                    sys.exit()
            else:
                self.execute_script(self.get_log_dml(True), connection)

        else:
            fname = r'cur_install.sql'
            fpath = path.abspath(f'{self.repo_properties.dist_path}/{self.get_dist_folder_name}/{fname}')

            with open(fpath, mode='wt', encoding='UTF-8') as f1:
                with open(resource_path(r'misc/start_single_statement.txt'), mode='rt', encoding='UTF-8') as f2:
                    st = f2.read()
                f1.write(st)

                for _, __, k in self.script_list:
                    f1.write(f'{self.read_sql(k)}\n\n')

                with open(resource_path(r'misc/end_single_statement.txt'), mode='rt', encoding='UTF-8') as f2:
                    st = f2.read()

                f1.write(st)
            try:
                self.log_and_print(f'Executing script: {fpath}','yellow')
                self.execute_script(self.read_sql(fpath), connection)
                self.execute_script(self.get_log_dml(True), connection)
            except Exception as e:
                self.execute_script(self.get_log_dml(False), connection)
                self.log_and_print(e,'red')
                self.log_and_print('Got errors during deploy execution, further execution is stopped','red')
                sys.exit()


if __name__ == '__main__':
    just_fix_windows_console()
    try:
        pg_builder = PostgresObjInstaller()
        pg_builder.clone_repo()
        pg_builder.switch_branch()
        pg_builder.check_folder_and_scripts()
        pg_builder.copy_scripts_to_dist_path()
        pg_builder.deploy_objects()
    except Exception:
        print(colored(sys.exc_info()[0],'red'))
        from traceback import format_exc

        print(colored(format_exc(),'red'))
    finally:
        cprint('Press Enter to close the window','light_red')
        input()

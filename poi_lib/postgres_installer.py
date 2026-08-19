import shutil
import sys
from collections import namedtuple
from datetime import datetime
from os import path, chmod, makedirs
from stat import S_IWRITE

from git import Repo
from psycopg2 import sql, errors
from termcolor import colored, cprint

from poi_lib import resource_path
from poi_lib.models import DeployType, RevertStage, DeployMode, RepositoryConfig, PGConnectionConfig
from poi_lib.postgres_client import PostgresClient


class PostgresObjInstaller:
    __log_file = r'install.log'
    __prompts_default = ['cyan', None, ['bold']]
    __encoding = r'UTF-8'
    __inst_file = r'objects.inst'
    __revert_file = r'objects.revert'
    __single_transaction_filename = r'cur_install.sql'
    __grants_filename = r'grants.sql'

    def __init__(self, config: dict):
        self.repo_config = RepositoryConfig(config['repo'])
        self.db_config = PGConnectionConfig(config['db']['connection'])
        self.pg_client = PostgresClient(self.db_config)
        self.repo_config.revert_branch = None
        self.repo = None
        self.script_list = None
        self.deploy_mode = DeployMode(config['misc']['deploy_mode']).value
        self.deploy_type = DeployType.RELEASE.value
        self.log_table = config['db']['log_table']
        self.__dist_folder_name = None
        self.__release_branch = None
        self.__revert_branch = None

    def __setattr__(self, key, value):
        if (key in self.__dict__ and value != '') or key not in self.__dict__:
            object.__setattr__(self, key, value)

    def __deploy_type_file_map(self, deploy_type):
        if deploy_type == DeployType.RELEASE.value:
            return self.__inst_file
        if deploy_type == DeployType.REVERT.value:
            return self.__revert_file
        raise ValueError('Invalid deploy type')

    def log_and_print(self, message, color, attrs=None):
        with open(f'{self.repo_config.dist_path}/{self.__dist_folder_name}/{self.__log_file}', mode='a',
                  encoding=self.__encoding) as f:
            f.write(f'{datetime.now()}: {message}\n')

        cprint(message, color=color, attrs=attrs)

    def clone_repo(self):
        cprint(f'Enter the remote repo path, default is: {self.repo_config.remote_path}', *self.__prompts_default)
        self.repo_config.remote_path = input().strip()

        cprint(f'Remote repo path is set to {self.repo_config.remote_path}', 'light_green')

        cprint(f'Enter the local path where repo will be cloned, default is: {self.repo_config.local_path}',
               *self.__prompts_default)
        self.repo_config.local_path = input().strip()

        cprint(f'Local repo path is set to {self.repo_config.local_path}', 'light_green')

        def remove_readonly(func, fpath, *args):
            chmod(fpath, S_IWRITE)
            func(fpath)

        try:
            cprint(f'Folder will be overwritten: {self.repo_config.local_path}', 'red', attrs=['bold'])
            shutil.rmtree(self.repo_config.local_path, onerror=remove_readonly)
        except FileNotFoundError:
            pass

        cprint('Cloning repository...', 'yellow')
        self.repo = Repo.clone_from(self.repo_config.remote_path, self.repo_config.local_path)
        cprint('Repository cloned successfully', 'light_green', attrs=['bold'])
        return self

    def handle_deploy_path(self):
        cprint(f'Select deploy type (release/revert) '
               f'Default type is: {self.deploy_type}', color='cyan', attrs=['bold'])
        self.deploy_type = input().strip()
        cprint(f'Deploy type is set to {DeployType(self.deploy_type).value}', 'light_green')

        if self.deploy_type == DeployType.RELEASE.value:
            self.switch_to_release_branch()
            self.create_dist_folder()
            self.check_folder_and_scripts()
            self.copy_scripts_to_dist_path()
        else:  # self.deploy_type == DeployType.REVERT.value
            self.switch_to_release_branch()
            self.create_dist_folder()
            self.check_folder_and_scripts()
            self.copy_scripts_to_dist_path(RevertStage.ONE.value)
            self.switch_to_revert_branch()
            self.copy_scripts_to_dist_path(RevertStage.TWO.value)
        return self

    def create_dist_folder(self):
        _format = '%Y-%d-%m %H.%M.%S'
        self.dist_folder_name = f'{self.deploy_type} {self.get_branch()} {datetime.now().strftime(_format)}'
        makedirs(path.abspath(
            fr'{self.repo_config.dist_path}/{self.dist_folder_name}'))

    def switch_to_release_branch(self):
        cprint(f'Enter a release branch name or commit SHA-1, default branch is: {self.repo_config.release_branch}',
               *self.__prompts_default)
        self.repo_config.release_branch = input().strip()
        cprint(f'Release branch/SHA-1 is set to {self.repo_config.release_branch}', 'light_green')
        cprint('Checking out...', 'yellow')
        self.repo.git.checkout(self.repo_config.release_branch)
        cprint('Checkout is successful', 'light_green')
        self.__release_branch = self.get_branch()

    def switch_to_revert_branch(self):
        cprint(f'Enter a revert branch name or commit SHA-1, default branch is: {self.repo_config.revert_branch}',
               *self.__prompts_default)
        self.repo_config.revert_branch = input().strip()
        cprint(f'Revert branch/SHA-1 is set to {self.repo_config.revert_branch}', 'light_green')
        cprint('Checking out...', 'yellow')
        self.repo.git.checkout(self.repo_config.revert_branch)
        cprint('Checkout is successful', 'light_green')
        self.__revert_branch = self.get_branch()

    Script = namedtuple('Script', ['repo_fpath', 'content_fpath', 'dist_fpath'])

    def check_scripts(self, script_list: list[Script]):
        for script in script_list:
            if not path.exists(script.repo_fpath):
                self.log_and_print(f'Specified script doesn\'t exist {script.content_fpath}', 'red')
                self.log_and_print('Fill objects.inst file with correct script paths and try again', 'red')
                sys.exit()
        return script_list

    def check_folder_and_scripts(self):
        cprint(
            f'Enter a subfolder name of Requests catalog(must contain {self.__deploy_type_file_map(self.deploy_type)} file)'
            f', default folder is: {self.repo_config.folder}', *self.__prompts_default)
        self.repo_config.folder = input().strip()

        cprint(f'Folder is set to {self.repo_config.folder}', 'light_green')

        inst_path = path.abspath(
            fr'{self.repo_config.local_path}/Requests/{self.repo_config.folder}/{self.__deploy_type_file_map(self.deploy_type)}')

        with open(inst_path, mode='rt', encoding=self.__encoding) as f:
            file_paths = [self.Script(path.abspath(fr'{self.repo_config.local_path}/{line.rstrip()}')
                                      , line.rstrip()
                                      , path.abspath(
                    fr'{self.repo_config.dist_path}/{self.dist_folder_name}/{line.rstrip()}'))
                          for line in f if not line.startswith('#')]

        self.script_list = self.check_scripts(file_paths)
        self.log_and_print(f'List of deploy scripts created successfully', 'light_green')

    def copy_scripts_to_dist_path(self, revert_stage=RevertStage.ZERO.value):
        if revert_stage == RevertStage.ZERO.value:
            cprint('Copying scripts to dist path...', 'yellow')
            for script in self.script_list:
                makedirs(path.dirname(script.dist_fpath), exist_ok=True)
                shutil.copy(script.repo_fpath, script.dist_fpath)
            else:
                cprint('Scripts copied successfully', 'light_green')
                cprint(fr'Deployment scripts location is {self.repo_config.dist_path}\{self.dist_folder_name}',
                       'light_magenta')

        elif revert_stage == RevertStage.ONE.value:
            stage_flag = False
            for i, script in enumerate(s for s in self.script_list if s.content_fpath.startswith('Requests')):
                if i == 0:
                    cprint('Copying first stage scripts to dist path...', 'yellow')
                    stage_flag = True
                makedirs(path.dirname(script.dist_fpath), exist_ok=True)
                shutil.copy(script.repo_fpath, script.dist_fpath)
            else:
                if stage_flag:
                    cprint('First stage scripts copied successfully', 'light_green')
        else:  # revert_stage==RevertStage.TWO.value
            for i, script in enumerate(s for s in self.script_list if s.content_fpath.startswith('OBJ')):
                if i == 0:
                    cprint('Copying second stage scripts to dist path...', 'yellow')
                makedirs(path.dirname(script.dist_fpath), exist_ok=True)
                shutil.copy(script.repo_fpath, script.dist_fpath)
            else:
                cprint('Scripts copied successfully', 'light_green')
                cprint(fr'Deployment scripts location is {self.repo_config.dist_path}\{self.dist_folder_name}',
                       'light_magenta')

    def read_sql(self, filepath):
        with open(filepath, mode='rt', encoding=self.__encoding) as f:
            sql_content = f.read()
        return sql_content

    def execute_script(self, sql_query, connection, *args):
        with connection.cursor() as cur:
            if args:
                cur.execute(sql_query, args)
                try:
                    res = cur.fetchone()
                except errors.ProgrammingError:
                    res = None
                return res
            else:
                cur.execute(sql_query)
                self.log_and_print('Success', 'magenta')
                return None

    def get_branch(self):
        try:
            return f'{self.repo.active_branch} {self.repo.head.commit}'
        except TypeError:
            return f'{self.repo.head.commit}'

    @property
    def _commit(self):
        return f'{self.repo.head.commit}'

    @property
    def _last_hash_query(self):
        schema, table = self.log_table.split('.')

        query = sql.SQL('''SELECT {field}
                        FROM {schema}.{table}
                        WHERE created=(select max(created) FROM {schema}.{table} )
                        AND deploy_type = %s 
                        AND is_successful'''
                        ).format(
            field=sql.Identifier('branch'),
            schema=sql.Identifier(schema),
            table=sql.Identifier(table)
        )
        return query

    def get_log_dml(self, is_successful):
        if self.deploy_type == DeployType.RELEASE.value:
            branch = repr(self.__release_branch)
        else:  # self.deploy_type==DeployType.REVERT.value:
            branch = repr(f'{self.__release_branch} to {self.__revert_branch}')

        return f'insert into {self.log_table}(branch, deploy_mode, is_successful, deploy_type, requests_folder) ' \
               f'values({branch}, {repr(self.deploy_mode)}, {is_successful}' \
               f', {repr(self.deploy_type)}, {repr(self.repo_config.folder)})'

    @property
    def dist_folder_name(self):
        return self.__dist_folder_name

    @dist_folder_name.setter
    def dist_folder_name(self, value):
        if self.__dist_folder_name is None:
            self.__dist_folder_name = value

    def create_single_inst_file(self) -> str | None:
        if self.deploy_mode == DeployMode.SINGLE_STATEMENT.value:
            fpath = path.abspath(f'{self.repo_config.dist_path}'
                                 f'/{self.dist_folder_name}'
                                 f'/{self.__single_transaction_filename}')

            with open(fpath, mode='wt', encoding=self.__encoding) as f1:
                with open(resource_path(r'misc/start_single_statement.txt'), mode='rt', encoding=self.__encoding) as f2:
                    st = f2.read()
                f1.write(st)

                for script in self.script_list:
                    f1.write(f'{self.read_sql(script.repo_fpath)}\n\n')

                with open(resource_path(r'misc/end_single_statement.txt'), mode='rt', encoding=self.__encoding) as f2:
                    st = f2.read()

                f1.write(st)
            return fpath
        return None

    def collect_grants(self, connection) -> str | None:
        import re
        objects_to_check = []
        # regex for DROP VIEW/FUNCTION/PROCEDURE/ROUTINE [IF EXISTS] [schema.]name
        identifier = r'(?:[a-zA-Z_][a-zA-Z0-9_$]*|"[^"]+")'
        drop_pattern = re.compile(
            fr'drop\s+(view|function|procedure|routine)\s+(?:if\s+exists\s+)?(?:({identifier})\.)?({identifier})',
            re.IGNORECASE
        )

        for script in self.script_list:
            content = self.read_sql(script.dist_fpath)
            matches = drop_pattern.finditer(content)
            for match in matches:
                obj_type = match.group(1).lower()
                schema = match.group(2)
                name = match.group(3)

                # Clean identifiers: remove double quotes if present, otherwise lowercase
                def clean_id(ident):
                    if ident is None:
                        return None
                    if ident.startswith('"') and ident.endswith('"'):
                        return ident[1:-1]
                    return ident.lower()

                schema = clean_id(schema) or 'public'
                name = clean_id(name)
                objects_to_check.append({'type': obj_type, 'schema': schema, 'name': name})

        if not objects_to_check:
            return None

        grant_statements = []
        for obj in objects_to_check:
            # For functions/procedures/routines we might need more to identify them (arguments), 
            # but simple name might work if they are unique or we fetch all with that name.
            # Routine in PG includes both functions and procedures.
            
            query = ""
            if obj['type'] == 'view':
                query = sql.SQL("""
                    SELECT 'GRANT ' || privilege_type || ' ON ' || table_schema || '.' || table_name || ' TO ' || grantee || ';'
                    FROM information_schema.role_table_grants
                    WHERE table_schema = %s AND table_name = %s
                    AND grantee != CURRENT_USER
                """)
            elif obj['type'] in ('function', 'procedure', 'routine'):
                # Using information_schema for routines to properly handle default privileges and overloads
                query = sql.SQL("""
                    SELECT 'GRANT ' || privilege_type || ' ON ROUTINE ' || n.nspname || '.' || p.proname || '(' || 
                           pg_get_function_identity_arguments(p.oid) || ') TO ' || grantee || ';'
                    FROM information_schema.routine_privileges rp
                    JOIN pg_namespace n ON n.nspname = rp.routine_schema
                    JOIN pg_proc p ON p.pronamespace = n.oid AND p.proname = rp.routine_name
                    WHERE rp.routine_schema = %s AND rp.routine_name = %s
                    AND rp.grantee != CURRENT_USER
                    AND rp.specific_name = p.proname || '_' || p.oid::text
                """)

            if query:
                with connection.cursor() as cur:
                    cur.execute(query, (obj['schema'], obj['name']))
                    rows = cur.fetchall()
                    for row in rows:
                        grant_statements.append(row[0])

        if grant_statements:
            grants_fpath = path.abspath(f'{self.repo_config.dist_path}/{self.dist_folder_name}/{self.__grants_filename}')
            with open(grants_fpath, mode='wt', encoding=self.__encoding) as f:
                f.write('\n'.join(grant_statements))
            return grants_fpath
        return None

    def deploy_objects(self):
        cprint(f'Execute scripts as single statement or separately (single/separate)? '
               f'Default mode is: {self.deploy_mode}', color='cyan', attrs=['bold'])
        self.deploy_mode = input().strip()
        cprint(f'Deploy mode is set to {self.deploy_mode}', 'light_green')

        if self.deploy_mode not in (_.value for _ in DeployMode):
            raise RuntimeError(colored('Invalid deploy mode', 'red', attrs=['bold']))

        fpath = self.create_single_inst_file()

        self.pg_client.prompt_connection_details(self.__prompts_default)
        connection = self.pg_client.ac_connection

        try:
            last_hash = self.execute_script(self._last_hash_query, connection, DeployType.RELEASE.value)
        except errors.UndefinedTable:
            last_hash = None

        if last_hash:
            last_hash = last_hash[0].split()[-1]

            if last_hash == self._commit:
                cprint(f'Commit {last_hash} is already installed last, do you want to proceed anyway?(y/n)'
                       , color='yellow', attrs=['bold'])
                answer = input().strip().lower()
                if answer == 'n':
                    sys.exit()

        grants_fpath = self.collect_grants(connection)

        if self.deploy_mode == DeployMode.SINGLE_STATEMENT.value:
            try:
                self.log_and_print(f'Executing script: {fpath}', 'yellow')
                # Use transactional connection for single statement if needed, 
                # but original code used ac_connection. 
                # However, PostgresClient now provides tx_connection too.
                # Original code just set autocommit=True.
                self.execute_script(self.read_sql(fpath), connection)
                if grants_fpath:
                    self.log_and_print(f'Executing grants script: {grants_fpath}', 'yellow')
                    self.execute_script(self.read_sql(grants_fpath), connection)

                cprint(f'Logging ci info...', 'yellow')
                self.execute_script(self.get_log_dml(True), connection)
            except Exception as e:
                cprint(f'Logging ci info...', 'yellow')
                self.execute_script(self.get_log_dml(False), connection)
                self.log_and_print(e, 'red')
                self.log_and_print('Got errors during deploy execution, further execution is stopped', 'red')
                sys.exit()
        else:
            for script in self.script_list:
                try:
                    self.log_and_print(f'Executing script: {script.content_fpath}', 'yellow')
                    self.execute_script(self.read_sql(script.dist_fpath), connection)
                except Exception as e:
                    cprint(f'Logging ci info...', 'yellow')
                    self.execute_script(self.get_log_dml(False), connection)
                    self.log_and_print(e, 'red')
                    self.log_and_print('Got errors during deploy execution, further execution is stopped', 'red')
                    sys.exit()
            else:
                if grants_fpath:
                    try:
                        self.log_and_print(f'Executing grants script: {grants_fpath}', 'yellow')
                        self.execute_script(self.read_sql(grants_fpath), connection)
                    except Exception as e:
                        self.log_and_print(f'Error applying grants: {e}', 'red')
                        # We might not want to exit here if deploy succeeded, but requirements say we should follow same pattern
                        cprint(f'Logging ci info...', 'yellow')
                        self.execute_script(self.get_log_dml(False), connection)
                        sys.exit()

                cprint(f'Logging ci info...', 'yellow')
                self.execute_script(self.get_log_dml(True), connection)
        
        self.pg_client.close()

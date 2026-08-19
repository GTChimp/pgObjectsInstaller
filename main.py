import sys
from os import path, environ
from pathlib import Path

def get_resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', str(Path(__file__).parent))
    return path.join(base_path, relative_path)

environ['GIT_PYTHON_GIT_EXECUTABLE'] = path.abspath(get_resource_path(r'misc/PortableGit-2.45.0-64-bit/bin/git.exe'))

from colorama import just_fix_windows_console
from termcolor import colored, cprint

from poi_lib import get_version, ConfigValidator, PostgresObjInstaller

if __name__ == '__main__':
    just_fix_windows_console()
    cprint(f'Postgres Objects Installer v.{get_version()}', 'white', 'on_blue', attrs=['bold'])
    try:
        validator = ConfigValidator()
        config = validator.validate_config()

        pg_builder = PostgresObjInstaller(config)
        pg_builder.clone_repo() \
            .handle_deploy_path() \
            .deploy_objects()
    except Exception:
        print(colored(sys.exc_info()[0], 'red'))
        from traceback import format_exc
        print(colored(format_exc(), 'red'))
    finally:
        cprint('Press Enter to close the window', 'light_red')
        input()

import os
import pkg_resources

import jinja2
from clint.textui import colored, prompt, puts, validators


db_ports = {
    'postgresql': '5432',
    'mysql': '3306',
    'mssql': '1433',
    'oracle': '1521',
    'firebird': '3050'
}

dialect_options = [{'selector': '1', 'prompt': 'PostgreSQL', 'return': 'postgresql'},
                   {'selector': '2', 'prompt': 'MySQL', 'return': 'mysql'},
                   {'selector': '3', 'prompt': 'SQL Server', 'return': 'mssql'},
                   {'selector': '4', 'prompt': 'Oracle', 'return': 'oracle'},
                   {'selector': '5', 'prompt': 'Firebird', 'return': 'firebird'},
                   {'selector': '6', 'prompt': 'SQLite', 'return': 'sqlite'}]


def path_query(query_string):
    path_txt = prompt.query(query_string, validators=[])
    return None if path_txt == '' else os.path.split(path_txt)


def setup_user_input():
    """
    Setup configuration and database loading script by querying information from user.
    """
    print "#### Please answer the following questions to setup the folder ####"
    log_folder = prompt.query('Logging folder (must exist):', default='.', validators=[validators.PathValidator()])
    loader_file = prompt.query('Loader file name:', default='loader')
    config_file = prompt.query('Config file name:', default='local')
    config_class = prompt.query('Config class name:', default='LocalConfig')

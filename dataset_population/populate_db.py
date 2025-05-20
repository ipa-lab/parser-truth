import time
import pandas
import sqlalchemy as db

import sys

from config import CSV_DATA_PATH, DB_CONNECTION_URL    
print("In module products sys.path[0], __package__ ==", sys.path[0], __package__)

column_name_mappings = {
    'project': {
        'project': 'github_url',
        'project_statements': 'statements'
    },
    'file': {
        'file': 'path',
        'github_link': 'github_url',
        'filename': 'name',
    },
    'method': {
        'method': 'name',
        'method_hash': 'hash',
        'original_loc': 'loc',
        'method_node_count': 'boa_cfg_nodes'
    }
}

slice_metadata_fields = [
    'parser_statement_count',
    'parsing_nodes',
    'input_variable',
    'input_is_tuple_assignment',
    'variable_count',
    'variable_names',
    'expression_count',
    'line_expression_counts',
    'function_calls_count',
    'function_names', 
    'functions',
    'first_function_line',
    'sequence_operations',
    'try_count',
    'except_count',
    'finally_count',
    'exception_types',
    'explicit_raise',
    'lines_with_exceptions',
    'contains_error_handling',
    'regex_patterns',
    'temp_vars',
    'method_chaining',
    'statement_count',
    'ast_nodes',
    'imports',
    'from_imports',
    'cyclomatic_complexity',
    'with_statements',
    'mod_operator',
    'list_comprehensions',
    'general_unpacking',
    'split_unpacking',
    'ternary_conditionals',
    'lambda_functions',
    'return_default',
    'dict_comprehensions',
    'f_strings'
]

df = pandas.read_csv(CSV_DATA_PATH)

# df = df.head(10000)

engine = db.create_engine(DB_CONNECTION_URL) # parameter echo=True for sqlalchemy logging

metadata = db.MetaData()


def init_db():
    create_tables()
    insert_data_in_db()
    return True


def create_tables():
    db.Table(
        'Project', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('github_link', db.String),
        db.Column('statements', db.Integer)
    )

    db.Table(
        'File', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('github_link', db.String),
        db.Column('name', db.String),
        db.Column('path', db.String),
        db.Column('loc', db.Numeric),
        db.Column('project_id', db.Integer, db.ForeignKey('Project.id'))
    )

    db.Table(
        'Method', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('name', db.String),
        db.Column('hash', db.Integer),
        db.Column('statements', db.Integer),
        db.Column('boa_cfg_nodes', db.Integer),
        db.Column('loc', db.Numeric),
        db.Column('file_id', db.Integer, db.ForeignKey('File.id'))
    )

    db.Table(
        'Slice', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('metadata', db.JSON),
        db.Column('code', db.String),
        db.Column('algo_type', db.String),
        db.Column('method_id', db.Integer, db.ForeignKey('Method.id'))
    )

    db.Table(
        'User', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('name', db.String),
        db.Column('pw_hash', db.String)
    )

    db.Table(
        'Audit', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('table', db.String),
        db.Column('field', db.String),
        db.Column('record_id', db.Integer),
        db.Column('old_value', db.String),
        db.Column('new_value', db.String),
        db.Column('add_by', db.Integer),
        db.Column('add_date', db.Date)
    )


    # Create all tables in the database
    metadata.create_all(engine)


def insert_data_in_db():
    with engine.begin() as connection:
        # projects
        unique_projects = df[['project', 'project_statements']] \
            .drop_duplicates(subset=['project']) \
            .rename(columns={
                'project': 'github_link',
                'project_statements': 'statements'
            })

        unique_projects.to_sql('Project', connection, if_exists='append', index=False)

        project_ids = pandas.read_sql("SELECT id AS project_id, github_link AS project FROM Project", connection)

        print('project IDs', project_ids)

        # files
        unique_files = df[['github_link', 'filename', 'file', 'loc', 'project']].drop_duplicates(subset=['github_link', 'filename'])

        project_files = unique_files.merge(project_ids, on='project')
        project_files[['github_link', 'filename', 'file', 'loc', 'project_id']] \
            .rename(columns={
                'filename': 'name',
                'file': 'path'}) \
            .to_sql('File', connection, if_exists='append', index=False)
        
        file_ids = pandas.read_sql("SELECT id AS file_id, path AS file FROM File", connection)

        print('file IDs', file_ids)

        # methods
        unique_methods = df[['method', 'method_hash', 'original_loc', 'method_node_count', 'file']] # todo probably also drop duplicates but on which field?

        file_methods = unique_methods.merge(file_ids, on='file')
        file_methods[['method', 'method_hash', 'original_loc', 'method_node_count', 'file_id']] \
            .rename(columns=column_name_mappings['method']) \
            .to_sql('Method', connection, if_exists='append', index=False)
        
        method_ids = pandas.read_sql("SELECT id AS method_id, name AS method FROM Method", connection)

        print('method IDs \n', method_ids)

        # slices
        method_slices = df[slice_metadata_fields + ['method']].merge(method_ids, on='method') # auf method oder method_hash überprüfen und probably auch drop_duplicates
        # .drop_duplicates().reset_index(drop=True)

        print(method_slices)
        print(time.strftime("%H:%M:%S %d.%m.%Y", time.localtime()))

        method_slices['metadata'] = method_slices[slice_metadata_fields].to_dict()

        method_slices.drop(columns= slice_metadata_fields + ["method"]) \
            .to_sql('Slice', connection, if_exists='append', index=False)

        
        print(time.strftime("%H:%M:%S %d.%m.%Y", time.localtime()))


    # insert user ?
    # user_df = df[['name', 'pw_hash']].drop_duplicates().reset_index(drop=True)
    # user_df.to_sql('User', con=engine, if_exists='append', index=False)

    # audit_df = df[['table', 'field', 'recordId', 'oldValue', 'newValue', 'addBy', 'addDate']].drop_duplicates().reset_index(drop=True)


if __name__ == '__main__':
    # test1.py executed as script
    create_tables()
    insert_data_in_db()

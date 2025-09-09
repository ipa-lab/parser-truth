from datetime import date
import os
import time
import pandas
import sqlalchemy as db

import sys

sys.path.append('../')

from config import CSV_DATA_PATH, DB_PATH, DB_CONNECTION_URL, INITIAL_USER_NAME, INITIAL_USER_PW_HASH

column_name_mappings = {
    'project': {
        'project': 'github_url',
        'project_statements': 'statements'
    },
    'file': {
        'github_link': 'github_url',
        'filename': 'name',
    },
    'method': {
        'method': 'name',
        'method_hash': 'hash',
        'original_loc': 'loc',
        'method_node_count': 'boa_cfg_nodes'
    },
    'slice': {
        'file': 'path',
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


def create_tables():
    engine = db.create_engine(DB_CONNECTION_URL) # parameter echo=True for sqlalchemy logging
    metadata = db.MetaData()

    db.Table(
        'Project', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('github_url', db.String),
        db.Column('statements', db.Integer)
    )

    db.Table(
        'File', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('github_url', db.String),
        db.Column('name', db.String),
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
        db.Column('path', db.String),
        db.Column('metadata', db.JSON),
        db.Column('code', db.String),
        db.Column('algo_type', db.String),
        db.Column('method_id', db.Integer, db.ForeignKey('Method.id'))
    )

    users = db.Table(
        'User', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('name', db.String),
        db.Column('pw_hash', db.String)
    )

    db.Table(
        'Dataset', metadata,
        db.Column('id', db.Integer, primary_key=True, autoincrement=True),
        db.Column('created_at', db.Date),
        db.Column('user_id', db.Integer, db.ForeignKey('User.id'))
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

    metadata.create_all(engine)

    with engine.connect() as conn:
        stmt = db.insert(users).values(
            name = INITIAL_USER_NAME,
            pw_hash = INITIAL_USER_PW_HASH,
        )
        
        conn.execute(stmt)
        conn.commit()


def insert_data_in_db():
    engine = db.create_engine(DB_CONNECTION_URL) # parameter echo=True for sqlalchemy logging
    metadata = db.MetaData()

    if len(sys.argv) == 3:
        df = pandas.read_csv(sys.argv[2])
    else:
        df = pandas.read_csv(CSV_DATA_PATH)

    user = sys.argv[1]
    user_table = db.Table('User', metadata, autoload_with=engine)
    
    with engine.begin() as conn:
        
        print(time.strftime('Start inserting data %H:%M:%S %d.%m.%Y', time.localtime()))

        # dataset
        user_query = db.select(user_table).where(user_table.c.name == user)
        user = conn.execute(user_query).first()

        if not user:
            print('ERROR: User does not exist.')
            sys.exit(1)
    
        pandas.DataFrame([{'user_id': user.id, 'created_at': date.today()}]) \
            .to_sql('Dataset', conn, if_exists='append', index=False)

        # projects
        df[['project', 'project_statements']] \
            .drop_duplicates(subset=['project']) \
            .rename(columns=column_name_mappings['project']) \
            .to_sql('Project', conn, if_exists='append', index=False)

        project_ids = pandas.read_sql("SELECT id AS project_id, github_url AS project FROM Project", conn)

        # files
        unique_files = df[['github_link', 'filename', 'loc', 'project']].drop_duplicates(subset=['github_link', 'filename'])

        project_files = unique_files.merge(project_ids, on='project')
        project_files[['github_link', 'filename', 'loc', 'project_id']] \
            .rename(columns=column_name_mappings['file']) \
            .to_sql('File', conn, if_exists='append', index=False)
        
        file_ids = pandas.read_sql("SELECT id AS file_id, name AS filename FROM File", conn)

        # methods
        unique_methods = df[['method', 'method_hash', 'original_loc', 'method_node_count', 'filename']] \
            .drop_duplicates() # todo probably also drop duplicates but on which field?

        file_methods = unique_methods.merge(file_ids, on='filename')
        file_methods[['method', 'method_hash', 'original_loc', 'method_node_count', 'file_id']] \
            .rename(columns=column_name_mappings['method']) \
            .to_sql('Method', conn, if_exists='append', index=False)
        
        method_ids = pandas.read_sql("SELECT Method.id AS method_id, Method.name AS method, File.name AS filename FROM Method JOIN File ON Method.file_id = File.id", conn)

        # slices
        method_slices = df[slice_metadata_fields + ['method', 'file', 'filename']].merge(method_ids, on=['method', 'filename']) 
        
        method_slices['metadata'] = method_slices[slice_metadata_fields].to_dict('index')

        method_slices.drop(columns= slice_metadata_fields + ['method', 'filename'], inplace=True)

        def read_file_content(row):
            try:
                with open(row['file'], 'r') as file:
                    return file.read()
            except FileNotFoundError:
                print(f"File not found: {row['file']}")
                return None
            except Exception as e:
                print(f"Error reading file {row['file']}: {e}")
                return None

        # Apply the function to each row and update the 'code' column
        method_slices['code'] = method_slices.apply(read_file_content, axis=1)
        
        method_slices = method_slices.map(str)

        method_slices.rename(columns={'file': 'path'}) \
            .to_sql('Slice', conn, if_exists='append', index=False)
        
        print('Inserted all data at: ' + time.strftime("%H:%M:%S %d.%m.%Y", time.localtime()))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('ERROR: missing arguments')
        print('Usage: python3 populate_db.py help | python3 populate_db.py <importedBy> <locationOfCSV>')
        sys.exit(1)
        
    if sys.argv[1] == 'help':
        print("Usage: python3 populate_db.py <importedBy> <locationOfCSV>")
        print("<importedBy> required")
        print("<locationOfCSV> optional, default '../data/analysis_results.csv'")
        sys.exit(0)
    
    if os.path.exists(DB_PATH):
        print(f"Database '{DB_PATH}' already exists. Continue with inserting data ...")
    else:
        create_tables()

    insert_data_in_db()

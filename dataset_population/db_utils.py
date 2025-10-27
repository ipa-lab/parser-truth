import sqlalchemy as db
import re
import os

from config import DB_CONNECTION_URL    


def run_sql_query(searchTable, searchParam=None):
    # searchParam = html.escape(searchParam)
    # searchParam = searchParam.replace(" ", "")
    engine = db.create_engine(DB_CONNECTION_URL) # parameter echo=True for sqlalchemy logging

    inspector = db.inspect(engine)
    if searchTable not in inspector.get_table_names():
        raise ValueError(f"Table {searchTable} does not exist")
    
    if not searchParam:
        sql = db.text('SELECT * from {}'.format(searchTable))

        with engine.connect() as conn:
            resultRows = conn.execute(sql).fetchall()

            return (searchTable, resultRows)
    
    pattern = r"^([a-zA-Z_]+)\s?(=|LIKE|>|<|>=|<=|!=|<>)\s?(\S+)$"
    match = re.match(pattern, searchParam.strip())
    
    if not match:
        raise ValueError("Invalid search condition format.")
    
    column_name, operator, value = match.groups()

    if column_name not in [col['name'] for col in inspector.get_columns(searchTable)]:
        raise ValueError(f"Column '{column_name}' doesn't exist in table '{searchTable}'")
    
    sql = db.text('SELECT * from {} WHERE {} {} :value'.format(searchTable, column_name, operator))

    with engine.connect() as conn:
        resultRows = conn.execute(sql, {'value': value}).fetchall()

        return (searchTable, resultRows)


def get_project_file_hierarchy(table, selectedRow):
    if not table and not selectedRow:
        raise ValueError("No Table or selected Row given.")
    
    engine = db.create_engine(DB_CONNECTION_URL)

    metadata = db.MetaData()
    
    project_table = db.Table('Project', metadata, autoload_with=engine)
    file_table = db.Table('File', metadata, autoload_with=engine)
    method_table = db.Table('Method', metadata, autoload_with=engine)
    slice_table = db.Table('Slice', metadata, autoload_with=engine)

    with engine.connect() as conn:
        match table:
            case 'File':
                project_query = db.select(project_table).where(project_table.c.id == selectedRow.project_id)
                selectedProject = conn.execute(project_query).first()
            case 'Method':
                project_query = db.select(project_table).\
                    join(file_table, project_table.c.id == file_table.c.project_id).\
                    where(file_table.c.id == selectedRow.file_id)
                selectedProject = conn.execute(project_query).first()
            case 'Slice':
                project_query = db.select(project_table).\
                    join(file_table, project_table.c.id == file_table.c.project_id).\
                    join(method_table, file_table.c.id == method_table.c.file_id).\
                    where(method_table.c.id == selectedRow.method_id)
                selectedProject = conn.execute(project_query).first()
            case _:
                selectedProject = selectedRow
        
        files_query = db.select(file_table.c.id, file_table.c.name, file_table.c.project_id).where(file_table.c.project_id == selectedProject.id)
        files = conn.execute(files_query).fetchall()
        
        file_methods = {}
        for file in files:
            methods_query = db.select(method_table.c.id, method_table.c.name, method_table.c.code, method_table.c.file_id).where(method_table.c.file_id == file.id)
            methods = conn.execute(methods_query).fetchall()
            file_methods[file.id] = methods
        
        method_slices = {}
        for methods in file_methods.values():
            for method in methods:
                slices_query = db.select(slice_table.c.id, slice_table.c.path, slice_table.c.method_id, slice_table.c.code).where(slice_table.c.method_id == method.id)
                slices = conn.execute(slices_query).fetchall()
                method_slices[method.id] = slices

        return (selectedProject, files, file_methods, method_slices)


def get_metadata_for_selection(resultTableName, selectedRow):
    (project, file, method, slice) = ([], [], [], [])
    
    engine = db.create_engine(DB_CONNECTION_URL)
    metadata = db.MetaData()
    # load & set project metadata

    if resultTableName == 'Slice':
        # TODO query to get slice metadata json
        print(selectedRow)


    return (project, file, method, slice)


if __name__ == '__main__':
    run_sql_query('SELECT * from PROJECT')

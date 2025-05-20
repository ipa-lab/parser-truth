import streamlit as st
import os
import sys
sys.path.append('..')
sys.path.append('../dataset_population')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dataset_population.populate_db import init_db
from dataset_population.db_utils import run_sql_query


st.text_input("Your SQL Query here", key="query")

# You can access the value at any point with:
st.session_state.query

if "initDbDone" not in st.session_state:
    st.session_state.initDbDone = init_db()

table = run_sql_query('SELECT * from PROJECT')

st.table(table)

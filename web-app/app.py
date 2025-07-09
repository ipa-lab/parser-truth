import streamlit as st
import os
import sys
sys.path.append('..')
sys.path.append('../dataset_population')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dataset_population.db_utils import get_code, get_project_file_hierarchy, run_sql_query

st.set_page_config(layout="wide")

search, code, details = st.columns([2,3,1])
resultRows = []
resultTableName = ''
selectedTableRow = {}

if 'selected_code_item' not in st.session_state:
    st.session_state.selected_code_item = None

if 'code' not in st.session_state:
    st.session_state.code = None

if 'details_metadata' not in st.session_state:
    st.session_state.details_metadata = None

## left column ##
with search:
    st.subheader('Search', divider="rainbow")

    searchTable = st.selectbox(
        "Select a Table", label_visibility="collapsed", placeholder="Table to search ...", index=None, options=("Project", "File", "Method", "Slice"),
    )

    searchCol, helpCol = st.columns([1, 0.13])
    with searchCol:
        searchParam = st.text_input("Put Your Search Paramaters here", label_visibility="collapsed", placeholder="Your Search Paramaters ...", icon="🔍", key="searchParam", max_chars=50)
    with helpCol:
        with st.popover('', icon="❔"):
            # IDEA maybe do it with st.dialog instead
            st.write("One search statement.")
            st.write("For example: name LIKE %OCR%")
            st.write("possible operators: =, LIKE, >, <, >=, <=, <>, !=")
            # TODO put picture of db table diagram

    if searchTable and not searchParam:
        (resultTableName, resultRows) = run_sql_query(searchTable)
    elif searchTable and searchParam:
        (resultTableName, resultRows) = run_sql_query(searchTable, searchParam)

    st.divider()

    if not resultRows:
        st.subheader('Result Table', divider="rainbow")
    if resultRows:
        st.subheader('Result Table: {}'.format(resultTableName), divider="rainbow")

        tableSelectionEvent = st.dataframe(resultRows, selection_mode="single-row", on_select="rerun", hide_index=True)

        if len(tableSelectionEvent.selection.rows) > 0:
            # TODO laod metadata
            # st.session_state.details_metadata = True
            selectedTableRow = resultRows[tableSelectionEvent.selection.rows[0]]
            st.session_state.code = get_code(resultTableName, selectedTableRow)

    st.divider()

    st.subheader('Explorer', divider="rainbow")

    (selectedProject, files, file_methods, method_slices) = ({}, [], {}, {})

    if selectedTableRow:
        (selectedProject, files, file_methods, method_slices) = get_project_file_hierarchy(resultTableName, selectedTableRow)

        if selectedProject:
            st.markdown('''#### :file_folder: Project {}'''.format(selectedProject.github_url))

            with st.container(height=400):
                for file in files:
                    fileExpander = st.expander(":page_facing_up: __{}__".format(file.name), expanded=False)
                    
                    if file.id in file_methods:
                        for method in file_methods[file.id]:
                            # method_button_style = "secondary" if st.session_state.selected_code_item == 'method_{}'.format(method.id) else "tertiary"

                            if fileExpander.button(
                                '''&emsp; 🔧 {}'''.format(method.name),
                                key=f"select_method_{method.id}",
                                type="tertiary"
                            ):
                                # st.session_state.selected_code_item = 'method_{}'.format(method.id)
                                selectedTableRow = selectedProject
                                st.session_state.code = get_code('Method', method)
                                # TODO load metadata/details
                            
                            if method.id in method_slices:
                                for slice in method_slices[method.id]:
                                    # slice_button_style = "secondary" if st.session_state.selected_code_item == 'slice_{}'.format(slice.id) else "tertiary"

                                    if fileExpander.button(
                                        '''&emsp; &emsp; 🔸 Slice :small[( {} )]'''.format(slice.path),
                                        key=f"select_slice_{slice.id}",
                                        type="tertiary"
                                    ):
                                        # st.session_state.selected_code_item = 'slice_{}'.format(slice.id)
                                        selectedTableRow = selectedProject
                                        st.session_state.code = get_code('Slice', slice)
                                        # TODO load metadata/details


## center ##
with code:
    if not st.session_state.code:
        st.subheader("Select a Code Snippet and the Code will display here", divider="rainbow")
    else:
        st.code(st.session_state.code, language="python", line_numbers=True)


## right column ##
with details:
    st.subheader('Details', divider="rainbow")
    if st.session_state.details_metadata:
        st.markdown('#### Project')

        st.divider()

        st.markdown('#### File')

        st.divider()

        st.markdown('#### Method')

        st.divider()

        st.markdown('#### Slice')

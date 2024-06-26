import streamlit as st
from streamlit_flow import streamlit_flow
from streamlit_flow.elements import StreamlitFlowNode, StreamlitFlowEdge
from google.cloud import bigquery
import config
import pandas as pd
from datetime import datetime, timezone

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

if 'page' not in st.session_state:
    st.session_state['page'] = 'list'

# BigQuery 클라이언트 설정
def get_bq_client():
    return bigquery.Client(location=config.get_config('bigquery.region'))

def load_dataflow_list():
    try:
        client = get_bq_client()
        query = """
            SELECT
                DF_ID,
                DF_NAME,
                `DESC`,
                CREATED_AT,
                UPDATED_AT
            FROM metatron.dataflow
            ORDER BY UPDATED_AT DESC
            """
        query_job = client.query(query)
        results = query_job.result()

        if results:
            columns = [field.name for field in results.schema]
            rows = [list(row.values()) for row in results]

        return columns, rows
    
    except Exception as e:
        print(f"Error: {e}")
        return None, None
    
    finally:
        client.close()
        print('BigQuery connection closed!')

@st.cache_data(ttl=300)
def get_dataset_list():
    try:
        client = get_bq_client()
        query = """
            SELECT DS_ID, TABLE_NAME
            FROM metatron.dataset
            ORDER BY TABLE_NAME ASC
            """
        query_job = client.query(query)
        results = query_job.result()
        dataset_list = []
        for row in results:
            dataset_list.append({'id': row[0], 'name': row[1]})
        return dataset_list
    
    except Exception as e:
        print(f"Error: {e}")
        return None, None
    finally:
        client.close()
        print('BigQuery connection closed!')

@st.cache_data(ttl=300)
def get_dataflow_dataset(df_id):
    try:
        client = get_bq_client()
        query = """
            SELECT DF.DF_ID, DF.DF_NAME, DF.DESC, DSDF.ID, DSDF.DS_ID, DS.DS_NAME, DS.DS_TYPE, DS.TABLE_NAME
            FROM (SELECT * FROM metatron.dataflow WHERE DF_ID = @df_id) AS DF
              LEFT JOIN metatron.dataset_dataflow AS DSDF
                ON DF.DF_ID = DSDF.DF_ID
              LEFT JOIN metatron.dataset AS DS
                ON DSDF.DS_ID = DS.DS_ID
            """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("df_id", "INT64", df_id)
            ]
        )

        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        dataflow_dataset_list = []
        for row in results:
            dataflow_dataset_list.append({'df_id': row[0], 'df_name': row[1], 'desc': row[2], 'ds_id': row[4], 'ds_name': row[5], 'ds_type': row[6], 'table_name': row[7]})

        return dataflow_dataset_list
    
    except Exception as e:
        print(f"Error: {e}")
        return None, None
    finally:
        client.close()
        print('BigQuery connection closed!')

def show_list():
    st.title('Dataflow 설정')

    if 'dataflow_list_columns' not in st.session_state or 'dataflow_list_rows' not in st.session_state:
        columns, rows = load_dataflow_list()
        st.session_state.dataflow_list_columns = columns
        st.session_state.dataflow_list_rows = rows
    else:
        columns = st.session_state.dataflow_list_columns
        rows = st.session_state.dataflow_list_rows
    
    st.write(f'{len(rows)}개의 데이터가 있습니다.')

    grid = st.container()

    df = pd.DataFrame(rows, columns=columns)
    grid.dataframe(df, use_container_width=True, hide_index=True, column_config={
            "DF_ID": "ID",
            "DF_NAME": "이름",
            "DESC": "설명",
            "CREATED_AT": "생성일시",
            "UPDATED_AT": "최종 수정일시"
        })
    
    if st.button("데이터플로우 추가"):
        @st.experimental_dialog("Dataflow 생성")
        def modal_dialog():
            dataset_list = get_dataset_list()
            dataflow_name = st.text_input("이름", key="dataflow_name")
            dataflow_desc = st.text_input("설명", key="dataflow_desc")
            # selected_dataset = st.selectbox("데이터셋을 선택해 주세요.", [dataset["TABLE_NAME"] for dataset in dataset_list], key="dataset_selector")
            options = {dataset["name"]: dataset for dataset in dataset_list}
            # selected_dataset = st.selectbox("데이터셋을 선택해 주세요.", options.keys())
            selected_dataset_name = st.selectbox("데이터셋을 선택해 주세요.", list(options.keys()))
            selected_dataset = options[selected_dataset_name]
            
            if st.button("완료", key="confirm_button"):
                if not dataflow_name or dataflow_name == "":
                    st.error("데이터플로우 이름을 입력해주세요.")
                elif not selected_dataset or selected_dataset["id"] == "":
                    st.error("데이터셋을 선택해주세요.")
                else:
                    try:
                        # dataflow 추가
                        query = """
                            SELECT COALESCE(MAX(df_id), 0) + 1 AS next_df_id
                            FROM `metatron.dataflow`
                        """
                        client = get_bq_client()
                        query_job = client.query(query)
                        next_df_id = [row.next_df_id for row in query_job.result()][0]
                        current_time = datetime.now(timezone.utc)
                        df = pd.DataFrame({
                            "df_id": [next_df_id],
                            "df_name": [dataflow_name],
                            "desc": [dataflow_desc],
                            "created_at": [current_time],
                            "updated_at": [current_time]
                        })

                        # dataset_dataflow 추가
                        query = """
                            SELECT COALESCE(MAX(id), 0) + 1 AS next_id
                            FROM `metatron.dataset_dataflow`
                        """
                        df_table_ref = client.dataset("metatron").table("dataflow")
                        df_table = client.get_table(df_table_ref)

                        query_job = client.query(query)
                        next_id = [row.next_id for row in query_job.result()][0]
                        dsdf = pd.DataFrame({
                            "id": [next_id],
                            "df_id": [next_df_id],
                            "ds_id": [selected_dataset["id"]],
                            "created_at": [current_time],
                            "updated_at": [current_time]
                        })

                        df_table_ref = client.dataset("metatron").table("dataflow")
                        df_table = client.get_table(df_table_ref)

                        dsdf_table_ref = client.dataset("metatron").table("dataset_dataflow")
                        dsdf_table = client.get_table(dsdf_table_ref)

                        with st.spinner("데이터플로우를 추가하는 중..."):
                            df_load_job = client.load_table_from_dataframe(df, df_table)
                            dsdf_load_job_ = client.load_table_from_dataframe(dsdf, dsdf_table)
                            df_load_job.result()
                            dsdf_load_job_.result()
                            
                        del st.session_state.dataflow_list_columns
                        del st.session_state.dataflow_list_rows
                        
                        st.rerun()
                    except Exception as e:
                        print(f"Error: {e}")
                        return None, None
                    finally:
                        client.close()
                        print('BigQuery connection closed!')
                    
        modal_dialog()
    
    if st.button("데이터플로우 상세보기"):
        @st.experimental_dialog("Dataflow 상세보기")
        def modal_dialog():
            dataflow_id = st.text_input("ID", key="dataflow_id")
            
            if st.button("상세보기", key="detail_button"):
                if not dataflow_id or dataflow_id == "":
                    st.error("데이터플로우 ID를 입력해주세요.")
                else:
                    st.session_state['selected_id'] = dataflow_id
                    st.session_state['page'] = 'details'
                    st.rerun()
                    
        modal_dialog()

def show_details():
    st.header('세부 내용 화면')
    selected_df_id = st.session_state.get('selected_id', 'N/A')
    print(f'선택한 Dataflow Id: {selected_df_id}')
    
    if st.button('목록 보기'):
        st.session_state['page'] = 'list'
        st.rerun()
    
    dataset_list = get_dataflow_dataset(selected_df_id)
    print('dataset list: ', dataset_list)

    nodes = [StreamlitFlowNode(id='1', pos=(100, 100), data={'id': 'text2sql-0', 'label': 'adot_applog_prd_all', 'type': 'IMPORTED', 'database': 'metatron', 'table': 'adot_applog_prd_all'}, node_type='input', source_position='right', draggable=False),
            StreamlitFlowNode('2', (275, 50), {'id': 'text2sql-1', 'label': '01. 데이터 클렌징', 'type': 'WRANGLED'}, 'default', 'right', 'left', draggable=False),
            StreamlitFlowNode('3', (275, 150), {'id': 'text2sql-2', 'label': '02. 데이터 집계', 'type': 'WRANGLED'}, 'default', 'right', 'left', draggable=False)]

    edges = [StreamlitFlowEdge('1-2', '1', '2', animated=True),
            StreamlitFlowEdge('1-3', '1', '3', animated=True)]

    selected_id = streamlit_flow('ret_val_flow',
                    nodes,
                    edges,
                    fit_view=True,
                    get_node_on_click=True,
                    get_edge_on_click=True)

    # TODO
    def add_new_dataset():
        print()

    def display_node_info(node_id):
        node = next((node for node in nodes if node.id == node_id), None)
        data = []
        if node:
            node_data = {
                '이름': node.data['label'],
                '타입': node.data['type']
            }
            if node.data['type'] == 'IMPORTED':
                node_data['데이터베이스'] = node.data['database']
                node_data['테이블'] = node.data['table']

            data.append(node_data)
            df = pd.DataFrame(data)

            st.dataframe(df)

            if node.data['type'] == 'IMPORTED':
                st.button("새로운 데이터셋 생성", type="primary", on_click=add_new_dataset)
            elif node.data['type'] == 'WRANGLED':
                id = node.data['id']
                link = st.link_button("룰 편집", type="primary", url=f"http://localhost:8501/sql_generator/?workflow_name={id}")

    if selected_id:
        display_node_info(selected_id)

# 현재 페이지 상태에 따라 다른 화면 표시
if st.session_state['page'] == 'list':
    show_list()
elif st.session_state['page'] == 'details':
    show_details()
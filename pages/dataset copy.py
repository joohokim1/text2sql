import streamlit as st
import pandas as pd
from google.cloud import bigquery
import config
from datetime import datetime, timezone
import pytz

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

kst = pytz.timezone('Asia/Seoul')

# BigQuery 클라이언트 설정
def get_bq_client():
    return bigquery.Client(location=config.get_config('bigquery.region'))

@st.cache_data(ttl=300)
def get_filtered_tables():
    try:
        client = get_bq_client()
        query = """
            SELECT TABLE_NAME AS TABLE_NAME
            FROM metatron.INFORMATION_SCHEMA.TABLES
            WHERE NOT STARTS_WITH (UPPER(TABLE_NAME), 'DATASET_')
            AND TABLE_NAME NOT IN ('adot_sql_generator_log', 'dataset', 'dataset_dataflow', 'dataflow', 'rule')
            ORDER BY TABLE_NAME ASC
            """
        query_job = client.query(query)
        results = query_job.result()
        filtered_tables = []
        for row in results:
            filtered_tables.append(row[0])
        return filtered_tables
    
    except Exception as e:
        print(f"Error: {e}")
        return None, None
    finally:
        client.close()
        print('BigQuery connection closed!')

def load_dataset_list():
    try:
        client = get_bq_client()
        query = """
            SELECT
                DS_ID,
                DS_NAME,
                DS_TYPE,
                TABLE_NAME,
                CREATED_AT,
                UPDATED_AT
            FROM metatron.dataset
            ORDER BY UPDATED_AT DESC
            """
        query_job = client.query(query)
        results = query_job.result()

        columns = [field.name for field in results.schema]
        rows = [list(row.values()) for row in results]

        return columns, rows
    
    except Exception as e:
        print(f"Error: {e}")
        return None, None
    
    finally:
        client.close()
        print('BigQuery connection closed!')

# def show_dataset_list(columns, rows):
#     if rows:
#         st.write(f"{len(rows)} 개 데이터가 있습니다")
#         df = pd.DataFrame(rows, columns=columns)
#         df_display = df.drop(columns=['DS_ID'])

#         st.dataframe(df_display, use_container_width=True, hide_index=True, column_config={
#             "DS_NAME": "이름",
#             "DS_TYPE": "타입",
#             "TABLE_NAME": "소스",
#             "CREATED_AT": "생성일시",
#             "UPDATED_AT": "최종 수정일시"
#         })

#         for index, row in df.iterrows():
#             col1, col2 = st.columns([9, 1])
#             with col1:
#                 st.write(row['DS_NAME'])
#             with col2:
#                 if st.button("상세보기", key=f"button_{index}"):
#                     show_dataset_detail(columns, row)

#     else:
#         st.write(f"데이터가 존재하지 않습니다")

def show_dataset_data(ds_id):
    if ds_id:
        @st.experimental_dialog("Dataset 정보", width="large")
        def modal_dialog():
            try:
                query1 = f"""
                WITH filtered_tables AS (
                    SELECT table_name
                    FROM `metatron.INFORMATION_SCHEMA.TABLES`
                    WHERE table_type = 'BASE TABLE'
                )
                SELECT table_name 
                FROM filtered_tables 
                WHERE table_name = (SELECT table_name FROM `metatron.dataset` WHERE ds_id = {ds_id})
                """
                client = get_bq_client()
                query_job1 = client.query(query1)
                results1 = query_job1.result()

                table_name = None
                for row in results1:
                    table_name = row["table_name"]

                if table_name:
                    query2 = query2 = f"""
                    SELECT * 
                    FROM `metatron.{table_name}`
                    LIMIT 5
                    """
                    
                    query_job2 = client.query(query2)
                    results2 = query_job2.result()

                    df = results2.to_dataframe()
                    st.write(df)

            except Exception as e:
                print(f"Error: {e}")
                return None, None
            finally:
                client.close()
                print('BigQuery connection closed!')

        modal_dialog()


def show_dataset_detail(columns, row):
    @st.experimental_dialog("Dataset 정보", width="large")
    def modal_dialog():
        try:
            print(columns.index('TABLE_NAME'))
            print(row[columns.index('TABLE_NAME')])
            table_name = row[columns.index('TABLE_NAME')]
            query1 = f"""
            SELECT * FROM `metatron.{table_name}` LIMIT 5
            """
            client = get_bq_client()
            query_job1 = client.query(query1)
            results1 = query_job1.result()

            df1 = results1.to_dataframe()
            print(df1)
            

            # table_name = None
            # for row in results1:
            #     table_name = row["table_name"]

            # if table_name:
            #     query2 = query2 = f"""
            #     SELECT * 
            #     FROM `metatron.{table_name}`
            #     LIMIT 5
            #     """
                
            #     query_job2 = client.query(query2)
            #     results2 = query_job2.result()

            #     df = results2.to_dataframe()
            #     st.write(df)

        except Exception as e:
            print(f"Error: {e}")
            return None, None
        finally:
            client.close()
            print('BigQuery connection closed!')

    modal_dialog()

def show_dataset_list(columns, rows):
    if rows:
        df = pd.DataFrame(rows, columns=columns)
        colms = st.columns((2,1,2,2,1))
        fields = ["이름", "타입", "소스", "최종 수정일시", "상세"]
        for col, field_name in zip(colms, fields):
            #header
            col.write(field_name)
        
        for index, row in enumerate(rows):
            col1, col2, col3, col4, col5 = st.columns((2,1,2,2,1))
            col1.write(row[columns.index('DS_NAME')])
            col2.write(row[columns.index('DS_TYPE')])
            col3.write(row[columns.index('TABLE_NAME')])
            updated_at_kst = row[columns.index('UPDATED_AT')].astimezone(kst)
            col4.write(updated_at_kst.strftime('%Y-%m-%d %H:%M:%S'))
            detail_button_phold = col5.empty()
            show_detail = detail_button_phold.button("상세", key=f"dataset_detail_{index}")
            if show_detail:
                # show_dataset_data(row[columns.index('DS_ID')])
                show_dataset_detail(columns, row)

def main():
    st.title("Dataset 설정")

    if 'dataset_list_columns' not in st.session_state or 'dataset_list_rows' not in st.session_state:
        columns, rows = load_dataset_list()
        st.session_state.dataset_list_columns = columns
        st.session_state.dataset_list_rows = rows
    else:
        columns = st.session_state.dataset_list_columns
        rows = st.session_state.dataset_list_rows
    
    # show_dataset_list(columns, rows)
    show_dataset_list(columns, rows)

    # "데이터셋 추가" 버튼 클릭 시 다이얼로그 표시
    if st.button("데이터셋 추가", type='primary'):
        @st.experimental_dialog("Dataset 생성")
        def modal_dialog():
            selected_table = st.selectbox("테이블을 선택해 주세요.", get_filtered_tables(), key="table_selector")
            dataset_name = st.text_input("이름", key="dataset_name")
            
            if st.button("완료", key="confirm_button"):
                if not selected_table or selected_table == "":
                    st.error("테이블을 선택해 주세요.")
                if not dataset_name or dataset_name == "":
                    st.error("데이터셋 이름을 입력해주세요.")
                else:
                    try:
                        query = """
                            SELECT COALESCE(MAX(ds_id), 0) + 1 AS next_ds_id
                            FROM `metatron.dataset`
                        """
                        client = get_bq_client()
                        query_job = client.query(query)
                        next_ds_id = [row.next_ds_id for row in query_job.result()][0]
                        current_time = datetime.now(timezone.utc)
                        df = pd.DataFrame({
                            "ds_id": [next_ds_id],
                            "ds_name": [dataset_name],
                            "ds_type": ['Imported'],
                            "table_name": [selected_table],
                            "created_at": [current_time],
                            "updated_at": [current_time]
                        })

                        table_ref = client.dataset("metatron").table("dataset")
                        table = client.get_table(table_ref)

                        with st.spinner("데이터셋을 추가하는 중..."):
                            load_job = client.load_table_from_dataframe(df, table)
                            load_job.result()

                        del st.session_state.dataset_list_columns
                        del st.session_state.dataset_list_rows
                        
                        st.rerun()
                    except Exception as e:
                        print(f"Error: {e}")
                        return None, None
                    finally:
                        client.close()
                        print('BigQuery connection closed!')
                    
        modal_dialog()
    


if __name__ == "__main__":
    main()
import streamlit as st
import pandas as pd
from google.cloud import bigquery
import config

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

# BigQuery 클라이언트 설정
def get_bq_client():
    return bigquery.Client(location=config.get_config('bigquery.region'))

def get_filtered_tables():
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


def show_dataset_detail(ds_id):
    print(ds_id)

def load_dataset_list():
    client = get_bq_client()
    query = """
        SELECT
            DS_ID,
            DS_NAME,
            DS_TYPE,
            TABLE_NAME,
            UPDATED_AT
        FROM metatron.dataset
        ORDER BY UPDATED_AT DESC
        """
    query_job = client.query(query)
    results = query_job.result()

    columns = [field.name for field in results.schema]
    rows = [list(row.values()) for row in results]
    
    if rows:
        st.write(f"{len(rows)} 개 데이터가 있습니다")
        df = pd.DataFrame(rows, columns=columns)
        df_display = df.drop(columns=['DS_ID'])

        df_display['Details'] = [f"-" for index in df_display.index]
        st.dataframe(df_display, use_container_width=True, hide_index=True, column_config={
            "DS_NAME": "이름",
            "DS_TYPE": "타입",
            "TABLE_NAME": "소스",
            "UPDATED_AT": "최종 수정일",
            "Details" : "상세보기"
        })
        
        for index, row in df.iterrows():
            if st.button(f"상세 보기", key=f"button_{index}"):
                show_dataset_detail(row['DS_ID'])
        

    else:
        st.write(f"데이터가 존재하지 않습니다")

def main():
    st.title("Dataset 설정")
    load_dataset_list()

    # "데이터셋 추가" 버튼 클릭 시 다이얼로그 표시
    if st.button("데이터셋 추가"):
        @st.experimental_dialog("Dataset 생성")
        def modal_dialog():
            with st.spinner("테이블 목록을 가져오는 중..."):
                tables = get_filtered_tables()
            selected_table = st.selectbox("테이블을 선택해 주세요.", tables, key="table_selector")

            if st.button("완료", key="confirm_button"):
                print(f"SELECTED TABLE : {selected_table}")
                st.rerun()

        modal_dialog()

if __name__ == "__main__":
    main()
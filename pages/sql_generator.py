import streamlit as st
import pandas as pd
import backend as be
from google.cloud import bigquery

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

# Get dataset id
ds_id = None
if 'ds_id' in st.session_state:
    ds_id = st.session_state['ds_id']
    print(f"ds_id is {ds_id}")
else:
    print('No ds_id exists!')
    st.warning('Dataset을 선택해 주세요.', icon="⚠️")
    # st.switch_page("pages/dataset.py")

# 세션 초기화
st.session_state.queries = []
st.session_state.results = []


def sql_generator():
    st.title("Text2SQL Generator")

    # 상단 레이아웃
    grid, rule = st.columns([3, 1])

    with grid:
        st.header("Result Grid")

    with rule:
        st.header("Rule")

    # 사용자 정의 CSS 삽입
    st.markdown("""
        <style>
        .small-font {
            font-size: 14px !important;
        }
        </style>
        """, unsafe_allow_html=True)

    # 이전 질의 표시
    if ds_id:
        print('Display previous question.')
        # Get previous questions and sqls
        query = f"""
            SELECT user_question, result_sql
            FROM metatron.rule 
            WHERE DS_ID = {ds_id} 
              AND APPLIED_YN = 'Y' 
            ORDER BY UPDATED_AT DESC
            """
        columns, results = be.execute_query_and_get_results(query)
        
        for row in results:
            st.session_state.queries.append((row[0], row[1]))
        
        if len(st.session_state.queries) != 0:
            user_question, sql_query = st.session_state.queries[len(st.session_state.queries)-1]
            print(f"질의 : {user_question}")
            columns, results = be.execute_query_and_get_results(sql_query)

            st.session_state.results.append((columns, results))
            grid.write(f"{len(columns)} Columns | {len(results)} Rows")
            df = pd.DataFrame(results, columns=columns)
            grid.dataframe(df, use_container_width=True)


    # 사용자 입력
    query = st.chat_input("질의할 내용을 입력해 주세요.")

    # 이전 질의 및 결과를 저장할 세션 상태 초기화
    if "queries" not in st.session_state:
        st.session_state.queries = []

    if "results" not in st.session_state:
        st.session_state.results = []

    def handle_execute_button(sql_query, idx):
        user_query = ''
        sql_query = ''
        columns = ''
        results = ''

        # 이전 질의 최초 실행
        if idx != '' and len(st.session_state.results) == 0:
            user_query, sql_query = st.session_state.queries[idx]

            if sql_query:
                columns, results = be.execute_query_and_get_results(sql_query)

                if len(st.session_state.results) <= idx:
                    # idx에 해당하는 인덱스가 없으면 빈 리스트로 초기화
                    while len(st.session_state.results) <= idx:
                        st.session_state.results.append([])

                    st.session_state.results[idx] = (columns, results)

        # 이전 질의 재실행
        elif idx != '':
            user_query, sql_query = st.session_state.queries[idx]
            columns, results = st.session_state.results[idx]

            print(f"질의 : {user_query}")
            print(f"쿼리 실행 : {sql_query}")
        # 새로운 질의 실행
        else: 
            print(f"질의 : {query}")

            # Claude API 호출하여 SQL 쿼리 생성
            user_query = query
            sql_query = be.get_sql_query_from_claude(user_query)

            if sql_query:
                columns, results = be.execute_query_and_get_results(sql_query)

                st.session_state.queries.append((query, sql_query))
                st.session_state.results.append((columns, results))

                # Save question and relative sql
                if idx and idx != "" and st.session_state.results[idx]:
                    # TODO: 기존 사용자 질의 최초 실행 시 세션에 넣는 작업
                    print()
                elif ds_id:
                    be.save_question(ds_id, user_query, sql_query.strip())
        
        rule.code(sql_query, language='sql')
        if results:
            grid.write(f"{len(columns)} Columns | {len(results)} Rows")
            df = pd.DataFrame(results, columns=columns)
            grid.dataframe(df, use_container_width=True)
            print("Successfully Queried!")

    if query:
        handle_execute_button(query , '')
        query = ''

    for i, (user_query, sql_query) in enumerate(st.session_state.queries):
        with st.expander(f"사용자 질의 {i + 1}", expanded=False):
            if st.button("▶︎ 재실행", key=f"execute_button_{i}"):
                print(f"Execute previous query: {i}")
                handle_execute_button(None, i)
            formatted_query = user_query.replace("\n", "<br>")
            st.markdown(f"<div class='small-font'>{formatted_query}</div>", unsafe_allow_html=True)


if __name__ == '__main__':
    sql_generator()
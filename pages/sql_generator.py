import streamlit as st
import pandas as pd
import backend as be

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

def sql_generator():
    def convert_newlines_to_br(text):
        return text.replace("\n", "<br>")


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

    # 사용자 입력
    query = st.chat_input("질의할 내용을 입력해 주세요.")

    # 이전 질의 및 결과를 저장할 세션 상태 초기화
    if "queries" not in st.session_state:
        st.session_state.queries = []

    if "results" not in st.session_state:
        st.session_state.results = []

    def handle_execute_button(sql_query, idx):
        sql_query = ''
        columns = ''
        results = ''

        if idx != '':
            user_query, sql_query = st.session_state.queries[i]
            columns, results = st.session_state.results[i]
        else: 
            # Claude API 호출하여 SQL 쿼리 생성
            sql_query = be.get_sql_query_from_claude(query)
            if sql_query:
                columns, results = be.execute_query_and_get_results(sql_query)

                st.session_state.queries.append((query, sql_query))
                st.session_state.results.append((columns, results))
        
        rule.code(sql_query, language='sql')
        if results:
            grid.write(f"{len(columns)} Columns | {len(results)} Rows")
            df = pd.DataFrame(results, columns=columns)
            grid.dataframe(df, use_container_width=True)


    if query:
        handle_execute_button(query , '')
        query = ''

    for i, (user_query, sql_query) in enumerate(st.session_state.queries):
        with st.expander(f"사용자 질의 {i + 1}", expanded=False):
            if st.button("▶︎ 재실행", key=f"execute_button_{i}"):
                handle_execute_button(sql_query, i)
            formatted_query = user_query.replace("\n", "<br>")
            st.markdown(f"<div class='small-font'>{formatted_query}</div>", unsafe_allow_html=True)


if __name__ == '__main__':
    sql_generator()
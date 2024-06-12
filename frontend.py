import streamlit as st
import pandas as pd
import backend as be

def convert_newlines_to_br(text):
    return text.replace("\n", "<br>")

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

# 사용자 정의 CSS 삽입
st.markdown("""
    <style>
    .small-font {
        font-size: 14px !important;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("Text2SQL Generator")
st.write("질의한 내용에 대한 SQL 쿼리와 결과를 보여줍니다.")

# 사용자 입력
query = st.chat_input("질의할 내용을 입력해 주세요.")

# 이전 질의 및 결과를 저장할 세션 상태 초기화
if "queries" not in st.session_state:
    st.session_state.queries = []

if "results" not in st.session_state:
    st.session_state.results = []

if query:
    # Claude API 호출하여 SQL 쿼리 생성
    sql_query = be.get_sql_query_from_claude(query)
    if sql_query:
        st.session_state.queries.append((query, sql_query))
        columns, results = be.execute_query_and_get_results(sql_query)
        st.session_state.results.append((columns, results))

# 이전 질의 및 결과 표시
for i, (user_query, sql_query) in enumerate(st.session_state.queries):
    with st.expander(f"사용자 질의 {i + 1}", expanded=False):
        formatted_query = convert_newlines_to_br(user_query)
        st.markdown(f"<div class='small-font'>{formatted_query}</div>", unsafe_allow_html=True)
    st.code(sql_query, language='sql')
    columns, results = st.session_state.results[i]
    if results:
        st.write("**조회 결과:**")
        df = pd.DataFrame(results, columns=columns)
        st.dataframe(df, use_container_width=True)

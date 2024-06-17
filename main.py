import streamlit as st 

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

st.title("Text2SQL Generator")

st.write("질의한 내용에 대한 SQL 쿼리와 결과를 보여줍니다.")
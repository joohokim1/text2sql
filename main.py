import streamlit as st
import backend as be

# Streamlit 설정
st.set_page_config(
    page_title="Text2SQL Generator",
    page_icon="🤖",
    layout="wide",
)

# 앱이 실행될 때 한 번만 호출되도록 설정
if 'initialized' not in st.session_state:
    be.on_app_start()
    st.session_state['initialized'] = True


st.title("Text2SQL Generator")

st.write("질의한 내용에 대한 SQL 쿼리와 결과를 보여줍니다.")
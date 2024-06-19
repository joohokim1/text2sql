import streamlit as st
from streamlit_flow import streamlit_flow
from streamlit_flow.elements import StreamlitFlowNode, StreamlitFlowEdge
import pandas as pd

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

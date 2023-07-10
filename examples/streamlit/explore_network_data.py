# -*- coding: utf-8 -*-

import streamlit as st

import kiara_plugin.streamlit as kst
from kiara.api import KiaraAPI

st.set_page_config(layout="wide")

kst.init()

api: KiaraAPI = st.kiara.api

with st.sidebar:
    result = st.kiara.context_switch_control(switch_to_selected=True, allow_create=True)

network_data = st.kiara.select_network_data(
    label="Please select a 'network_data' value", preview=False, add_create_widget=True
)

if not network_data:
    st.stop()

    nodes_file = st.kiara.onboard_file(
        label="Nodes file", help="Provide a file with nodes data."
    )

    if not nodes_file:
        st.stop()

    results = api.run_job("create.table.from.file", inputs={"file": nodes_file})
    nodes_table = results.get_value_obj("table")

    inputs = {
        "nodes": nodes_table,
    }
    api.run_job("assemble.network_data", inputs=inputs)


st.kiara.preview_network_data(network_data)

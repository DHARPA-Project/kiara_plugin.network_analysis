# -*- coding: utf-8 -*-

# run with:
#
# streamlit run examples/streamlit/network_analysis/create_graphs.py

import os
import tempfile
from typing import Iterable, List, Tuple

import networkx as nx
import streamlit as st
import streamlit.components.v1 as components
from kiara import Kiara
from kiara.interfaces.python_api.operation import KiaraOperation
from kiara.models.values.value import Value
from kiara_plugin.tabular.models.table import KiaraTableMetadata
from pyvis.network import Network
from streamlit.uploaded_file_manager import UploadedFile

from kiara_plugin.network_analysis.models import NetworkData

st.title("Kiara/streamlit experiment - create a network graph")

kiara_obj = Kiara.instance()


def import_bytes(kiara: Kiara, uploaded_file: UploadedFile):

    with tempfile.TemporaryDirectory() as tmpdirname:
        path = os.path.join(tmpdirname, uploaded_file.name)
        with open(path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        op = KiaraOperation(kiara=kiara, operation_name="create.table.from.csv_file")
        inputs = {"csv_file": path}
        job_id = op.queue_job(**inputs)

        op.save_result(
            job_id=job_id, aliases={"table": uploaded_file.name.replace(".", "__")}
        )

        # imported: ValueSet = kiara.run(  # type: ignore
        #     "file.import_from.local.file_path",
        #     inputs={"source": path},
        # )
        # file_item = imported.get_value_obj("value_item")
        # assert file_item is not None
        # file_item.save(aliases=[uploaded_file.name.replace(".", "__")])


def onboard_file(kiara: Kiara, uploaded_file):

    if uploaded_file:
        if isinstance(uploaded_file, UploadedFile):
            import_bytes(kiara=kiara, uploaded_file=uploaded_file)
        else:
            for x in uploaded_file:
                import_bytes(kiara=kiara, uploaded_file=x)


def find_all_aliases_of_type(kiara: Kiara, value_type: str) -> List[str]:

    result = []
    for alias, alias_item in kiara.alias_registry.aliases.items():
        value = kiara.data_registry.get_value(alias_item.value_id)
        if value.data_type_name == value_type:
            result.append(alias)

    return result


def table_mgmt(kiara: Kiara):

    fp = st.sidebar.file_uploader(
        "Import table(s) from csv file(s)", type=["csv"], accept_multiple_files=True
    )
    if fp:
        onboard_file(kiara=kiara, uploaded_file=fp)

    st.sidebar.write("## All your tables:")
    all_table_aliases = find_all_aliases_of_type(kiara, value_type="table")
    if not all_table_aliases:
        st.sidebar.write(" -- no tables --")
    else:
        for a in all_table_aliases:
            st.sidebar.write(a)


def graphs_list(kiara: Kiara):

    st.sidebar.write("## All your graphs:")
    all_graph_aliases = find_all_aliases_of_type(kiara, value_type="network_data")
    if not all_graph_aliases:
        st.sidebar.write(" -- no graphs --")
    else:
        for a in all_graph_aliases:
            st.sidebar.write(a)


def create_graph(kiara: Kiara):
    def create_graph(
        alias, edges, nodes, source_column, target_column, weight_column, node_index
    ) -> Tuple[str, Value]:

        st.write("GRAPH")
        if not alias:
            return ("No alias specified, doing nothing...", None)

        all_graph_aliases = find_all_aliases_of_type(kiara, value_type="network_data")
        if alias in all_graph_aliases:
            return (f"Alias '{alias}' already registered.", None)

        if not edges:
            return ("No edges table specified, doing nothing...", None)

        inputs = {
            "edges": f"alias:{edges}",
            "nodes": f"alias:{nodes}",
            "source_column_name": source_column,
            "target_column_name": target_column,
            "id_column_name": node_index,
        }

        try:
            op = KiaraOperation(
                kiara=kiara, operation_name="create.network_data.from.tables"
            )

            job_id = op.queue_job(**inputs)
            result = op.retrieve_result(job_id=job_id)

            op.save_result(job_id=job_id, aliases={"network_data": alias})

            network_data = result.get_value_obj("network_data")

            return (f"Saved network graph as: {alias}.", network_data)
        except Exception as e:
            return (f"Error creating graph: {e}", None)

        return ("CREATED GRAPH", None)

    def get_table_column_names(alias):

        if not alias:
            return []
        value = kiara.data_registry.get_value(f"alias:{alias}")
        if not value:
            return []

        md: KiaraTableMetadata = value.get_property_data("metadata.table")
        return md.table.column_names

    def find_likely_index(options: Iterable, keyword: str):

        for idx, alias in enumerate(options):
            if keyword.lower() in alias.lower():
                return idx

        return 0

    graph = None

    st.write("Create a new graph")

    graph_alias = st.text_input("The alias for the graph")
    all_table_aliases = find_all_aliases_of_type(kiara, value_type="table")

    default_edge_table = find_likely_index(all_table_aliases, "edge")
    default_node_table = find_likely_index(all_table_aliases, "node")

    select_edges = st.selectbox("Edges", all_table_aliases, index=default_edge_table)
    select_nodes = st.selectbox("Nodes", all_table_aliases, index=default_node_table)

    edge_column_names = get_table_column_names(select_edges)
    nodes_column_names = get_table_column_names(select_nodes)

    default_source_name = find_likely_index(edge_column_names, "source")
    default_target_name = find_likely_index(edge_column_names, "target")
    default_weight_name = find_likely_index(edge_column_names, "weight")
    default_id_name = find_likely_index(nodes_column_names, "Id")

    source_column_name = st.selectbox(
        "Source column name", edge_column_names, index=default_source_name
    )
    target_column_name = st.selectbox(
        "Target column name", edge_column_names, index=default_target_name
    )
    weight_column_name = st.selectbox(
        "Weight column name", edge_column_names, index=default_weight_name
    )
    nodes_index_name = st.selectbox(
        "Nodes table_index", nodes_column_names, index=default_id_name
    )

    create_button = st.button(label="Create graph")
    if create_button:
        result, graph = create_graph(
            alias=graph_alias,
            edges=select_edges,
            nodes=select_nodes,
            source_column=source_column_name,
            target_column=target_column_name,
            weight_column=weight_column_name,
            node_index=nodes_index_name,
        )
        st.info(result)

    if graph is None:
        return

    st.write("Graph properties")
    for prop in graph.property_names:
        with st.expander(prop):
            data = graph.get_property_data(prop)
            st.write(data.dict())

    network_data: NetworkData = graph.data
    vis_graph = Network(
        height="465px", bgcolor="#222222", font_color="white", directed=True
    )
    # add or remove "directed=True" above for displaying a directed graph with directed edges (with arrows)
    vis_graph.from_nx(network_data.as_networkx_graph(graph_type=nx.DiGraph))
    # change networkX graph type above to "Graph", "DiGraph", "MultiGraph", "MultiDiGraph" according to graph type
    # add this line "vis_graph.set_edge_smooth('dynamic')" to display parallel edges, otherwise they will be stacked upon each other and thus be invisible

    # Generate network with specific layout settings
    vis_graph.repulsion(
        node_distance=420,
        central_gravity=0.33,
        spring_length=110,
        spring_strength=0.10,
        damping=0.95,
    )

    # Save and read graph as HTML file (on Streamlit Sharing)
    try:
        path = "/tmp"
        vis_graph.save_graph(f"{path}/pyvis_graph.html")
        HtmlFile = open(f"{path}/pyvis_graph.html", "r", encoding="utf-8")

    # Save and read graph as HTML file (locally)
    except Exception:
        path = "/html_files"
        vis_graph.save_graph(f"{path}/pyvis_graph.html")
        HtmlFile = open(f"{path}/pyvis_graph.html", "r", encoding="utf-8")

    # Load HTML file in HTML component for display on Streamlit page
    components.html(HtmlFile.read(), height=800, width=800)


table_mgmt(kiara=kiara_obj)
create_graph(kiara=kiara_obj)
graphs_list(kiara=kiara_obj)

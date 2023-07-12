# -*- coding: utf-8 -*-
from typing import Iterable

from streamlit.delta_generator import DeltaGenerator

from kiara_plugin.network_analysis.defaults import (
    EDGE_ID_COLUMN_NAME,
    EDGES_TABLE_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    NODES_TABLE_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.streamlit.components.preview import PreviewComponent, PreviewOptions


class NetworkDataPreview(PreviewComponent):
    """Preview a value of type 'network data'.

    Currently, this displays a graph, as well as the nodes and edges tables. The graph is only a preview, and takes a while to render depending on the network data size, this will replaced at some point.
    """

    _component_name = "preview_network_data"

    @classmethod
    def get_data_type(cls) -> str:
        return "network_data"

    def render_preview(self, st: DeltaGenerator, options: PreviewOptions):

        import networkx as nx
        import streamlit.components.v1 as components
        from pyvis.network import Network

        from kiara_plugin.network_analysis.models import NetworkData

        _value = self.api.get_value(options.value)

        if options.show_properties:
            tab_names = ["Nodes", "Edges", "Graph", "Value properties"]
        else:
            tab_names = ["Nodes", "Edges", "Graph"]

        network_data: NetworkData = _value.data
        tabs = st.tabs(tab_names)

        with tabs[0]:
            nodes_table = network_data.get_table(NODES_TABLE_NAME)
            _callback, _key = self._create_session_store_callback(
                options, "preview", "network_data", "nodes"
            )
            show_internal_columns = tabs[0].checkbox(
                "Show computed columns", value=False, key=_key, on_change=_callback
            )
            if show_internal_columns:
                exclude_columns: Iterable[str] = []
            else:
                exclude_columns = (
                    x
                    for x in nodes_table.column_names
                    if x not in [NODE_ID_COLUMN_NAME, LABEL_COLUMN_NAME]
                    and x.startswith("_")
                )
            tabs[0].dataframe(
                nodes_table.to_pandas_dataframe(exclude_columns=exclude_columns),
                use_container_width=True,
                hide_index=True,
            )

        with tabs[1]:
            edges_table = network_data.get_table(EDGES_TABLE_NAME)
            _callback, _key = self._create_session_store_callback(
                options, "preview", "network_data", "edges"
            )
            show_internal_columns = tabs[1].checkbox(
                "Show computed columns", value=False, key=_key, on_change=_callback
            )
            if show_internal_columns:
                exclude_columns = []
            else:
                exclude_columns = (
                    x
                    for x in edges_table.column_names
                    if x
                    not in [EDGE_ID_COLUMN_NAME, SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
                    and x.startswith("_")
                )
            tabs[1].dataframe(
                edges_table.to_pandas_dataframe(exclude_columns=exclude_columns),
                use_container_width=True,
                hide_index=True,
            )

        # graph
        with tabs[2]:
            _callback, _key = self._create_session_store_callback(
                options, "preview", "network_data", "graphs"
            )
            graph_types = ["non-directed", "directed"]
            graph_type = tabs[2].radio(
                "Graph type", graph_types, key=_key, on_change=_callback
            )
            if graph_type == "non-directed":
                graph = network_data.as_networkx_graph(nx.Graph)
            else:
                graph = network_data.as_networkx_graph(nx.DiGraph)

            for node_id, data in graph.nodes(data=True):
                data["label"] = data.pop(LABEL_COLUMN_NAME)

            vis_graph = Network(
                height="400px", width="100%", bgcolor="#222222", font_color="white"
            )
            vis_graph.from_nx(graph)
            vis_graph.repulsion(
                node_distance=420,
                central_gravity=0.33,
                spring_length=110,
                spring_strength=0.10,
                damping=0.95,
            )

            html = vis_graph.generate_html()
            components.html(html, height=435)
        if options.show_properties:
            with tabs[3]:
                comp = self.get_component("display_value_properties")
                comp.render_func(tabs[3])(value=options.value)

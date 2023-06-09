# -*- coding: utf-8 -*-
import os

from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara_plugin.network_analysis.models import NetworkData

KIARA_METADATA = {
    "authors": [{"name": "Markus Binsteiner", "email": "markus@frkl.io"}],
    "description": "Modules related to extracting components from network data.",
}


class ExportNetworkDataModule(DataExportModule):
    """Export network data items."""

    _module_type_name = "export.network_data"

    def export__network_data__as__graphml_file(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as graphml file."""

        import networkx as nx

        target_path = os.path.join(base_path, f"{name}.graphml")

        # TODO: can't just assume digraph
        graph: nx.Graph = value.as_networkx_graph(
            nx.DiGraph, incl_node_attributes=True, incl_edge_attributes=True
        )
        nx.write_graphml(graph, target_path)

        return {"files": target_path}

    def export__network_data__as__gexf_file(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as gexf file."""

        import networkx as nx

        target_path = os.path.join(base_path, f"{name}.gexf")

        # TODO: can't just assume digraph
        graph: nx.Graph = value.as_networkx_graph(
            nx.DiGraph, incl_node_attributes=True, incl_edge_attributes=True
        )
        nx.write_gexf(graph, target_path)

        return {"files": target_path}

    def export__network_data__as__adjlist_file(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as adjacency list file."""

        import networkx as nx

        target_path = os.path.join(base_path, f"{name}.adjlist")

        # TODO: can't just assume digraph
        graph: nx.Graph = value.as_networkx_graph(
            nx.DiGraph, incl_node_attributes=True, incl_edge_attributes=True
        )
        nx.write_adjlist(graph, target_path)

        return {"files": target_path}

    def export__network_data__as__multiline_adjlist_file(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as multiline adjacency list file."""

        import networkx as nx

        target_path = os.path.join(base_path, f"{name}.adjlist_multiline")

        # TODO: can't just assume digraph
        graph: nx.Graph = value.as_networkx_graph(
            nx.DiGraph, incl_node_attributes=True, incl_edge_attributes=True
        )
        nx.write_multiline_adjlist(graph, target_path)

        return {"files": target_path}

    def export__network_data__as__edgelist_file(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as edgelist file."""

        import networkx as nx

        target_path = os.path.join(base_path, f"{name}.edge_list")

        # TODO: can't just assume digraph
        graph: nx.Graph = value.as_networkx_graph(
            nx.DiGraph, incl_node_attributes=True, incl_edge_attributes=True
        )
        nx.write_edgelist(graph, target_path)

        return {"files": target_path}

    def export__network_data__as__network_text_file(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as network text file (with a '.network' extension)."""

        import networkx as nx

        target_path = os.path.join(base_path, f"{name}.network")

        # TODO: can't just assume digraph
        graph: nx.Graph = value.as_networkx_graph(
            nx.DiGraph, incl_node_attributes=True, incl_edge_attributes=True
        )
        nx.write_network_text(graph, target_path)

        return {"files": target_path}

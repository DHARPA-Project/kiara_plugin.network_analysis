# -*- coding: utf-8 -*-


from kiara.modules import KiaraModule
from kiara_plugin.network_analysis.models import GraphRankingData

KIARA_METADATA = {
    "authors": [
        {"name": "Caitlin Burge", "email": "caitlin.burge@uni.lu"},
    ],
    "description": "Modules to produce ranking data for network graphs.",
}


class Degree_Ranking(KiaraModule):
    """Creates an ordered table with the rank and raw score for degree and weighted degree.

    Unweighted degree centrality uses an undirected graph and measures the number of independent connections each node has.
    Weighted degree centrality uses a directed graph and measures the total number of connections or weight attached to a node.

    Uses networkx degree.
    https://networkx.org/documentation/stable/reference/generated/networkx.classes.function.degree.html
    """

    _module_type_name = "calculate.graph.degree_ranking"

    def create_inputs_schema(self):

        return {
            "network_data": {
                "type": "network_data",
                "doc": "The network graph being queried.",
            },
        }

    def create_outputs_schema(self):
        return {
            "graph_ranking_data": {
                "type": "graph_ranking_data",
                "doc": "Data containing graph degree centrality information.",
            },
        }

    def process(self, inputs, outputs):

        import rustworkx as rx

        from kiara_plugin.network_analysis.models import NetworkData

        edges = inputs.get_value_obj("network_data")

        network_data: NetworkData = (
            edges.data
        )  # check the source for the NetworkData class to see what

        # we create a rustworkx graph, since rustworkx can do degrees, and should be quicker with larger graphs
        rg = network_data.as_rustworkx_graph(
            rx.PyGraph, multigraph=False, omit_self_loops=True
        )

        degrees = {}
        for node in rg.node_indexes():
            degrees[node] = rg.degree(node)

        ranking_data = GraphRankingData.create_from_ranking_dict(degrees)
        outputs.set_values(graph_ranking_data=ranking_data)

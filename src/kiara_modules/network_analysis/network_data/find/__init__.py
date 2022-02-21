# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from pydantic import Field, validator

from kiara_modules.network_analysis.metadata_models import NetworkData


class FindLargestComponentsModuleConfig(ModuleTypeConfigSchema):

    find_largest_component: bool = Field(
        description="Find the largest component of a graph.", default=True
    )

    number_of_components: bool = Field(
        description="Count the number of components.", default=True
    )


class GrpahComponentsModule(KiaraModule):
    """Extract component information from a graph.

    In particular, this module can calculate the number of components of a graph, and extract the largest sub-component
    from it.
    """

    _config_cls = FindLargestComponentsModuleConfig
    _module_type_name = "components"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"network_data": {"type": "network_data", "doc": "The network graph."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        result = {}
        if self.get_config_value("find_largest_component"):
            result["largest_component"] = {
                "type": "network_graph",
                "doc": "The largest connected component of the graph, as a new graph.",
            }

        if self.get_config_value("number_of_components"):
            result["number_of_components"] = {
                "type": "integer",
                "doc": "The number of components in the graph.",
            }

        return result

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        import networkx as nx

        network_data: NetworkData = inputs.get_value_data("network_data")
        input_graph: nx.Graph = network_data.as_networkx_graph(nx.Graph)

        if self.get_config_value("find_largest_component"):
            undir_graph = nx.to_undirected(input_graph)
            undir_components = nx.connected_components(undir_graph)
            lg_component = max(undir_components, key=len)
            subgraph = input_graph.subgraph(lg_component)
            outputs.set_values(largest_component=subgraph)

        if self.get_config_value("number_of_components"):
            undir_graph = nx.to_undirected(input_graph)
            number_of_components = nx.number_connected_components(undir_graph)

            outputs.set_values(number_of_components=number_of_components)


class FindShortestPathModuleConfig(ModuleTypeConfigSchema):

    mode: str = Field(
        description="Whether to calculate one shortest path for only one pair ('single-pair'), or use two node lists as input and select one of the following strategies: shortest path for each pair ('one-to-one'), the shortest path to all targets ('one-to-many'), or a matrix of all possible combinations ('many-to-many').",
        default="single-pair",
    )

    @validator("mode", allow_reuse=True)
    def _validate_mode(cls, v):

        allowed = ["single-pair", "one-to-one", "one-to-many", "many-to-many"]
        if v not in allowed:
            raise ValueError(f"'mode' must be one of: [{allowed}]")
        return v


class FindShortestPathModule(KiaraModule):
    """Find the shortest path between two nodes in a network graph."""

    _config_cls = FindShortestPathModuleConfig
    _module_type_name = "find_shortest_path"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        mode = self.get_config_value("mode")
        if mode == "single-pair":
            return {
                "network_data": {"type": "network_data", "doc": "The network graph"},
                "source_node": {"type": "any", "doc": "The id of the source node."},
                "target_node": {"type": "any", "doc": "The id of the target node."},
            }
        else:
            return {
                "network_data": {"type": "network_data", "doc": "The network graph"},
                "source_nodes": {"type": "list", "doc": "The ids of the source nodes."},
                "target_nodes": {"type": "list", "doc": "The ids of the target nodes."},
            }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        mode = self.get_config_value("mode")
        if mode == "single-pair":
            return {
                "path": {"type": "list", "doc": "The shortest path between two nodes."}
            }
        else:
            return {
                "paths": {
                    "type": "dict",
                    "doc": "A dict of dicts with 'source', 'target' and 'path' keys.",
                }
            }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        import networkx as nx

        mode = self.get_config_value("mode")
        if mode != "single-pair":
            raise NotImplementedError()

        network_data: NetworkData = inputs.get_value_data("network_data")
        graph: nx.Graph = network_data.as_networkx_graph(graph_type=nx.Graph)
        source: typing.Any = inputs.get_value_data("source_node")
        target: typing.Any = inputs.get_value_data("target_node")

        if source not in graph.nodes:
            raise KiaraProcessingException(
                f"Can't process shortest path, source '{source}' not in graph."
            )

        if target not in graph.nodes:
            raise KiaraProcessingException(
                f"Can't process shortest path, target '{target}' not in graph."
            )

        shortest_path = nx.shortest_path(graph, source=source, target=target)
        outputs.set_value("path", shortest_path)

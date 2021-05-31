# -*- coding: utf-8 -*-
import copy
import typing
from enum import Enum

import networkx
import networkx as nx
import pyarrow
from kiara import KiaraModule
from kiara.data.types import ValueType
from kiara.data.values import ValueSchema, ValueSet
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import KiaraModuleConfig
from networkx import DiGraph, Graph
from pydantic import Field, validator


class NetworkGraphType(ValueType):
    def validate(cls, value: typing.Any) -> typing.Any:

        if not isinstance(value, networkx.Graph):
            raise ValueError(f"Invalid type '{type(value)}' for graph: {value}")
        return value

    def extract_type_metadata(
        cls, value: typing.Any
    ) -> typing.Mapping[str, typing.Any]:

        graph: nx.Graph = value
        return {
            "directed": isinstance(value, DiGraph),
            "number_of_nodes": len(graph.nodes),
            "number_of_edges": len(graph.edges),
            "density": nx.density(graph),
        }


class GraphTypesEnum(Enum):

    undirected = "undirected"
    directed = "directed"
    multi_directed = "multi_directed"
    multi_undirected = "multi_undirected"


class CreateGraphConfig(KiaraModuleConfig):
    class Config:
        use_enum_values = True

    graph_type: typing.Optional[str] = Field(
        description="The type of the graph. If not specified, a 'graph_type' input field will be added which will default to 'directed'.",
        default=None,
    )

    @validator("graph_type")
    def _validate_graph_type(cls, v):

        try:
            GraphTypesEnum[v]
        except Exception:
            raise ValueError("Invalid graph type name: {v}")

        return v


class CreateGraphFromEdgesTableModule(KiaraModule):
    """Create a directed network graph object from tabular data."""

    _config_cls = CreateGraphConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs = {
            "edges_table": {
                "type": "table",
                "doc": "The table to extract the edges from.",
            },
            "source_column": {
                "type": "string",
                "default": "source",
                "doc": "The name of the column that contains the edge source in edges table.",
            },
            "target_column": {
                "type": "string",
                "default": "target",
                "doc": "The name of the column that contains the edge target in the edges table.",
            },
            "weight_column": {
                "type": "string",
                "default": "weight",
                "doc": "The name of the column that contains the edge weight in edges table.",
            },
        }

        if self.get_config_value("graph_type") is None:
            inputs["graph_type"] = {
                "type": "string",
                "default": "directed",
                "doc": "The type of the graph. Allowed: 'undirected', 'directed', 'multi_directed', 'multi_undirected'.",
            }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {
            "graph": {"type": "network_graph", "doc": "The (networkx) graph object."},
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        if self.get_config_value("graph_type") is not None:
            _graph_type = self.get_config_value("graph_type")
        else:
            _graph_type = inputs.get_value_data("graph_type")

        graph_type = GraphTypesEnum[_graph_type]

        edges_table_value = inputs.get_value_obj("edges_table")
        edges_table_obj: pyarrow.Table = edges_table_value.get_value_data()

        source_column = inputs.get_value_data("source_column")
        target_column = inputs.get_value_data("target_column")
        weight_column = inputs.get_value_data("weight_column")

        errors = []
        if source_column not in edges_table_obj.column_names:
            errors.append(source_column)
        if target_column not in edges_table_obj.column_names:
            errors.append(target_column)
        if weight_column not in edges_table_obj.column_names:
            errors.append(weight_column)

        if errors:
            raise KiaraProcessingException(
                f"Can't create network graph, source table missing column(s): {', '.join(errors)}. Available columns: {', '.join(edges_table_obj.column_names)}."
            )

        min_table = edges_table_obj.select(
            (source_column, target_column, weight_column)
        )
        pandas_table = min_table.to_pandas()

        if graph_type != GraphTypesEnum.directed:
            raise NotImplementedError("Only 'directed' graphs supported at the moment.")
        graph_cls = nx.DiGraph

        graph: nx.DiGraph = nx.from_pandas_edgelist(
            pandas_table,
            source_column,
            target_column,
            edge_attr=True,
            create_using=graph_cls,
        )
        outputs.set_value("graph", graph)


class AugmentNetworkGraphModule(KiaraModule):
    """Augment an existing graph with node attributes."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "graph": {"type": "network_graph", "doc": "The network graph"},
            "node_attributes": {
                "type": "table",
                "doc": "The table containing node attributes.",
                "optional": True,
            },
            "index_column_name": {
                "type": "string",
                "doc": "The name of the column that contains the node index in the node attributes table.",
                "optional": True,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"graph": {"type": "network_graph", "doc": "The network graph"}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        nodes_table_value = inputs.get_value_obj("node_attributes")

        if nodes_table_value.is_none or not nodes_table_value:
            # we return the graph as is
            # we are using the 'get_value_obj' method, because there is no need to retrieve the
            # actual data at all
            outputs.set_value("graph", inputs.get_value_obj("graph"))
            return

        input_graph: Graph = inputs.get_value_data("graph")
        graph: Graph = copy.deepcopy(input_graph)

        nodes_table_obj: pyarrow.Table = nodes_table_value.get_value_data()
        nodes_table_index = inputs.get_value_data("index_column_name")
        if nodes_table_index not in nodes_table_obj.column_names:
            raise KiaraProcessingException(
                f"Node attribute table does not have a column with (index) name '{nodes_table_index}'. Available column names: {', '.join(nodes_table_obj.column_names)}"
            )

        attr_dict = (
            nodes_table_obj.to_pandas()
            .set_index(nodes_table_index)
            .to_dict("index")
            .items()
        )
        graph.add_nodes_from(attr_dict)

        outputs.set_value("graph", graph)


class AddNodesToNetworkGraphModule(KiaraModule):
    """Add nodes to an existing graph."""

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {
            "graph": {"type": "network_graph", "doc": "The network graph"},
            "nodes": {
                "type": "table",
                "doc": "The table containing node attributes.",
                "optional": True,
            },
            "index_column_name": {
                "type": "string",
                "doc": "The name of the column that contains the node index in the node attributes table.",
                "optional": True,
            },
        }

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:
        return {"graph": {"type": "network_graph", "doc": "The network graph"}}

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        nodes_table_value = inputs.get_value_obj("node_attributes")

        if nodes_table_value.is_none:
            # we return the graph as is
            # we are using the 'get_value_obj' method, because there is no need to retrieve the
            # actual data at all
            outputs.set_value("graph", inputs.get_value_obj("graph"))
            return

        input_graph: Graph = inputs.get_value_data("graph")
        graph: Graph = copy.deepcopy(input_graph)

        nodes_table_obj: pyarrow.Table = nodes_table_value.get_value_data()
        nodes_table_index = inputs.get_value_data("index_column_name")

        attr_dict = (
            nodes_table_obj.to_pandas()
            .set_index(nodes_table_index)
            .to_dict("index")
            .items()
        )
        graph.add_nodes_from(attr_dict)

        outputs.set_value("graph", graph)


class FindShortestPathModuleConfig(KiaraModuleConfig):

    mode: str = Field(
        description="Whether to calculate one shortest path for only one pair ('single-pair'), or use two node lists as input and select one of the following strategies: shortest path for each pair ('one-to-one'), the shortest path to all targets ('one-to-many'), or a matrix of all possible combinations ('many-to-many').",
        default="single-pair",
    )

    @validator("mode")
    def _validate_mode(cls, v):

        allowed = ["single-pair", "one-to-one", "one-to-many", "many-to-many"]
        if v not in allowed:
            raise ValueError(f"'mode' must be one of: [{allowed}]")
        return v


class FindShortestPathModule(KiaraModule):
    """Find the shortest path between two nodes in a graph."""

    _config_cls = FindShortestPathModuleConfig

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        mode = self.get_config_value("mode")
        if mode == "single-pair":
            return {
                "graph": {"type": "network_graph", "doc": "The network graph"},
                "source_node": {"type": "any", "doc": "The id of the source node."},
                "target_node": {"type": "any", "doc": "The id of the target node."},
            }
        else:
            return {
                "graph": {"type": "network_graph", "doc": "The network graph"},
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
                "path": {"type": "array", "doc": "The shortest path between two nodes."}
            }
        else:
            return {
                "paths": {
                    "type": "table",
                    "doc": "A table with 'source', 'target' and 'path' column.",
                }
            }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        mode = self.get_config_value("mode")
        if mode != "single-pair":
            raise NotImplementedError()

        graph: Graph = inputs.get_value_data("graph")
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


class ExtractGraphPropertiesModuleConfig(KiaraModuleConfig):

    find_largest_component: bool = Field(
        description="Find the largest component of a graph.", default=True
    )
    number_of_nodes: bool = Field(
        description="Count the number of nodes.", default=True
    )
    number_of_edges: bool = Field(description="Count the number of edges", default=True)
    density: bool = Field(description="Calculate the graph density.", default=True)


class ExtractGraphPropertiesModule(KiaraModule):
    """Extract inherent properties of a network graph."""

    _config_cls = ExtractGraphPropertiesModuleConfig
    _module_type_name = "graph_properties"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        return {"graph": {"type": "network_graph", "doc": "The network graph."}}

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        result = {}
        if self.get_config_value("find_largest_component"):
            result["largest_component"] = {
                "type": "network_graph",
                "doc": "A sub-graph of the largest component of the graph.",
            }
            result["density_largest_component"] = {
                "type": "float",
                "doc": "The density of the largest component.",
            }

        if self.get_config_value("number_of_nodes"):
            result["number_of_nodes"] = {
                "type": "integer",
                "doc": "The number of nodes in the graph.",
            }

        if self.get_config_value("number_of_edges"):
            result["number_of_edges"] = {
                "type": "integer",
                "doc": "The number of edges in the graph.",
            }

        if self.get_config_value("density"):
            result["density"] = {"type": "float", "doc": "The density of the graph."}

        return result

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        graph: Graph = inputs.get_value_data("graph")

        if self.get_config_value("find_largest_component"):
            lc_graph = copy.deepcopy(graph)
            # largest_component = max(nx.strongly_connected_components_recursive(lc_graph), key=len)
            lc_graph.remove_nodes_from(
                list(nx.isolates(lc_graph))
            )  # remove unconnected nodes from graph
            lc_density = nx.density(lc_graph)
            outputs.set_values(
                largest_component=lc_graph, density_largest_component=lc_density
            )

        if self.get_config_value("number_of_nodes"):
            outputs.set_values(number_of_nodes=len(graph.nodes))

        if self.get_config_value("number_of_edges"):
            outputs.set_values(number_of_edges=len(graph.edges))

        if self.get_config_value("density"):
            density = nx.density(graph)
            outputs.set_values(density=density)

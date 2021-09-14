# -*- coding: utf-8 -*-
import copy
import os
import typing
from enum import Enum

import networkx as nx
from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import Value, ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara.module_config import ModuleTypeConfigSchema
from kiara.operations.extract_metadata import ExtractMetadataModule
from kiara.operations.save_value import SaveValueTypeModule
from kiara_modules.core.metadata_schemas import FileMetadata
from networkx import Graph
from pydantic import BaseModel, Field, validator

from kiara_modules.network_analysis.metadata_schemas import GraphMetadata


class GraphTypesEnum(Enum):

    undirected = "undirected"
    directed = "directed"
    multi_directed = "multi_directed"
    multi_undirected = "multi_undirected"


DEFAULT_SAVE_GRAPH_EDGES_TABLE_NAME = "edges.feather"
DEFAULT_SAVE_GRAPH_NODES_TABLE_NAME = "nodes.feather"
DEFAULT_SAVE_GRAPH_SOURCE_COLUMN_NAME = "source"
DEFAULT_SAVE_GRAPH_TARGET_COLUMN_NAME = "target"
DEFAULT_SAVE_GRAPH_WEIGHT_COLUMN_NAME = "weight"
DEFAULT_SAVE_GRAPH_NODES_TABLE_INDEX_COLUMN_NAME = "id"


class SaveGraphDataTypeModule(SaveValueTypeModule):
    """Save a network graph object."""

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "network_graph"

    _module_type_name = "save"

    def save_value(self, value: Value, base_path: str) -> typing.Dict[str, typing.Any]:

        import pyarrow as pa
        from pyarrow import feather

        graph: nx.Graph = value.get_value_data()

        graph_type = "directed"

        input_values = {
            "base_path": base_path,
            "edges_file_format": "feather",
            "nodes_file_format": "feather",
            "source_column": DEFAULT_SAVE_GRAPH_SOURCE_COLUMN_NAME,
            "target_column": DEFAULT_SAVE_GRAPH_TARGET_COLUMN_NAME,
            "weight_column": DEFAULT_SAVE_GRAPH_WEIGHT_COLUMN_NAME,
            "nodes_table_index": DEFAULT_SAVE_GRAPH_NODES_TABLE_INDEX_COLUMN_NAME,
            "graph_type": graph_type,
        }

        os.makedirs(base_path, exist_ok=True)

        edges_file_name = f"{DEFAULT_SAVE_GRAPH_EDGES_TABLE_NAME}.feather"
        edges_path = os.path.join(base_path, edges_file_name)
        df = nx.to_pandas_edgelist(graph, "source", "target")
        edges_table = pa.Table.from_pandas(df, preserve_index=False)

        # edge_attr_keys = set([k for n in graph.edges for k in graph.edges[n].keys()])
        # edge_attr_keys.add(weight_column_name)

        feather.write_feather(edges_table, edges_path)
        input_values["edges_path"] = edges_file_name

        nodes_file_name = f"{DEFAULT_SAVE_GRAPH_NODES_TABLE_NAME}.feather"
        nodes_path = os.path.join(base_path, nodes_file_name)

        node_attr_keys = set([k for n in graph.nodes for k in graph.nodes[n].keys()])

        if DEFAULT_SAVE_GRAPH_NODES_TABLE_INDEX_COLUMN_NAME in node_attr_keys:
            node_attr_keys.remove(DEFAULT_SAVE_GRAPH_NODES_TABLE_INDEX_COLUMN_NAME)

        nodes_dict: typing.Dict[str, typing.List[typing.Any]] = {
            DEFAULT_SAVE_GRAPH_NODES_TABLE_INDEX_COLUMN_NAME: []
        }
        for k in node_attr_keys:
            nodes_dict[k] = []

        for node in graph.nodes:
            nodes_dict[DEFAULT_SAVE_GRAPH_NODES_TABLE_INDEX_COLUMN_NAME].append(node)
            for k in node_attr_keys:
                attr = graph.nodes[node].get(k, None)
                nodes_dict[k].append(attr)

        nodes_table = pa.Table.from_pydict(nodes_dict)
        feather.write_feather(nodes_table, nodes_path)
        input_values["nodes_path"] = nodes_file_name

        load_config = {
            "base_path_input_name": "base_path",
            "module_type": "network_graph.load",
            "inputs": input_values,
            "output_name": "graph",
        }

        return load_config


SUPPORTED_INPUT_FILE_TYPES = ["auto", "graphml"]


class CreateGraphFromFileModule(KiaraModule):
    """Create a graph object from a file."""

    _module_type_name = "from_file"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "file": {
                "type": "file",
                "doc": "The file that contains the graph data.",
                "optional": False,
            },
            "input_type": {
                "type": "string",
                "doc": f"The input file type, supported: {', '.join(SUPPORTED_INPUT_FILE_TYPES)}",
                "default": "auto",
            },
        }
        return inputs

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "graph": {"type": "network_graph", "doc": "The network graph."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        input_file_type = inputs.get_value_data("input_type")
        input_file: FileMetadata = inputs.get_value_data("file")

        if input_file_type == "auto":
            if input_file.orig_filename.endswith(".graphml"):
                input_file_type = "graphml"
            else:
                input_file_type = "graphml"
                # raise NotImplementedError()

        if input_file_type == "graphml":
            graph = nx.read_graphml(input_file.path)
        else:
            raise KiaraProcessingException(
                f"Invalid input type '{input_file_type}'. Supported: {', '.join(SUPPORTED_INPUT_FILE_TYPES)}"
            )

        outputs.set_value("graph", graph)


class CreateGraphConfig(ModuleTypeConfigSchema):
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
    """Create a directed network graph object from table data."""

    _config_cls = CreateGraphConfig
    _module_type_name = "from_edges_table"

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

        import pyarrow as pa

        if self.get_config_value("graph_type") is not None:
            _graph_type = self.get_config_value("graph_type")
        else:
            _graph_type = inputs.get_value_data("graph_type")

        graph_type = GraphTypesEnum[_graph_type]

        edges_table_value = inputs.get_value_obj("edges_table")
        edges_table_obj: pa.Table = edges_table_value.get_value_data()

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
            raise NotImplementedError("Only 'directed' graph supported at the moment.")
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

    _module_type_name = "augment"

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

        import pyarrow as pa

        nodes_table_value = inputs.get_value_obj("node_attributes")

        if nodes_table_value.is_none or not nodes_table_value:
            # we return the graph as is
            # we are using the 'get_value_obj' method, because there is no need to retrieve the
            # actual data at all
            outputs.set_value("graph", inputs.get_value_obj("graph"))
            return

        input_graph: Graph = inputs.get_value_data("graph")
        graph: Graph = copy.deepcopy(input_graph)

        nodes_table_obj: pa.Table = nodes_table_value.get_value_data()
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

    _module_type_name = "add_nodes"

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

        import pyarrow as pa

        nodes_table_value = inputs.get_value_obj("node_attributes")

        if nodes_table_value.is_none:
            # we return the graph as is
            # we are using the 'get_value_obj' method, because there is no need to retrieve the
            # actual data at all
            outputs.set_value("graph", inputs.get_value_obj("graph"))
            return

        input_graph: Graph = inputs.get_value_data("graph")
        graph: Graph = copy.deepcopy(input_graph)

        nodes_table_obj: pa.Table = nodes_table_value.get_value_data()
        nodes_table_index = inputs.get_value_data("index_column_name")

        attr_dict = (
            nodes_table_obj.to_pandas()
            .set_index(nodes_table_index)
            .to_dict("index")
            .items()
        )
        graph.add_nodes_from(attr_dict)

        outputs.set_value("graph", graph)


class FindShortestPathModuleConfig(ModuleTypeConfigSchema):

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


class ExtractGraphPropertiesModuleConfig(ModuleTypeConfigSchema):

    number_of_nodes: bool = Field(
        description="Count the number of nodes.", default=True
    )
    number_of_edges: bool = Field(description="Count the number of edges", default=True)
    density: bool = Field(description="Calculate the graph density.", default=True)
    degrees: bool = Field(
        description="Calculate the graph degrees metrics.", default=True
    )
    shortest_path: bool = Field(
        description="Calculate the graph shortest path.", default=True
    )


class ExtractGraphPropertiesModule(KiaraModule):
    """Extract inherent properties of a network graph."""

    _config_cls = ExtractGraphPropertiesModuleConfig
    _module_type_name = "properties"

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

        result: typing.Dict[str, typing.Any] = {}

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

        if self.get_config_value("degrees"):
            result["average_degree"] = {
                "type": "float",
                "optional": True,
                "doc": "Average degree of the graph if it is not directed.",
            }
            result["average_in_degree"] = {
                "type": "float",
                "optional": True,
                "doc": "Average in degree of the graph if it is directed.",
            }
            result["average_out_degree"] = {
                "type": "float",
                "optional": True,
                "doc": "Average out degree of the graph if it is directed.",
            }

        if self.get_config_value("shortest_path"):
            result["average_shortest_path_length"] = {
                "type": "float",
                "optional": True,
                "doc": "Average shortest path length.",
            }

        return result

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        graph: Graph = inputs.get_value_data("graph")

        if self.get_config_value("number_of_nodes"):
            outputs.set_values(number_of_nodes=len(graph.nodes))

        if self.get_config_value("number_of_edges"):
            outputs.set_values(number_of_edges=len(graph.edges))

        if self.get_config_value("density"):
            density = nx.density(graph)
            outputs.set_values(density=density)

        if self.get_config_value("degrees"):

            nodes_count: int = graph.number_of_nodes()

            if nodes_count > 0:
                if nx.is_directed(graph):
                    digraph = typing.cast(nx.DiGraph, graph)
                    outputs.set_values(
                        average_in_degree=sum(d for _, d in digraph.in_degree())
                        / float(nodes_count),
                        average_out_degree=sum(d for _, d in digraph.out_degree())
                        / float(nodes_count),
                    )
                else:
                    outputs.set_values(
                        average_degree=sum(d for _, d in graph.degree())
                        / float(nodes_count)
                    )
        if self.get_config_value("shortest_path"):
            if nx.is_weakly_connected(graph):
                outputs.set_values(
                    average_shortest_path_length=nx.average_shortest_path_length(graph)
                )


class GraphMetadataModule(ExtractMetadataModule):
    """Extract metadata from a network graph object."""

    _module_type_name = "metadata"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "network_graph"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "network_graph"

    def _get_metadata_schema(
        self, type: str
    ) -> typing.Union[str, typing.Type[BaseModel]]:
        return GraphMetadata

    def extract_metadata(self, value: Value) -> typing.Mapping[str, typing.Any]:

        graph: nx.Graph = value.get_value_data()

        # TODO: check for other types
        if isinstance(graph, nx.DiGraph):
            graph_type = GraphTypesEnum.directed.value
        else:
            graph_type = GraphTypesEnum.undirected.value

        return {
            "graph_type": graph_type,
            "number_of_nodes": len(graph.nodes),
            "number_of_edges": len(graph.edges),
            "density": nx.density(graph),
        }


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
                "doc": "The largest connected component of the graph, as a new graph.",
            }

        if self.get_config_value("number_of_components"):
            result["number_of_components"] = {
                "type": "integer",
                "doc": "The number of components in the graph.",
            }

        return result

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        # TODO: check whether we need to deepcopy the graph first, but I don't think so

        input_graph: Graph = inputs.get_value_data("graph")
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


class AddCentralityCalculationsModule(KiaraModule):
    """Add centrality properties to a graph."""

    KIARA_METADATA = {
        "authors": [{"name": "Roman Kalyakin", "email": "roman@kalyakin.com"}],
    }

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        return {
            "graph": ValueSchema(type="network_graph"),
            "degree_property_name": ValueSchema(type="string", default="degree"),
            "indegree_property_name": ValueSchema(type="string", default="indegree"),
            "outdegree_property_name": ValueSchema(type="string", default="outdegree"),
            "isolated_property_name": ValueSchema(type="string", default="isolated"),
            "betweenness_property_name": ValueSchema(
                type="string", default="betweenness"
            ),
            "eigenvector_property_name": ValueSchema(
                type="string", default="eigenvector"
            ),
        }

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        return {
            "graph": ValueSchema(type="network_graph"),
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        graph: Graph = inputs.get_value_data("graph")
        graph = copy.deepcopy(graph)

        # degree
        degree_dict = graph.degree()
        nx.set_node_attributes(
            graph, dict(degree_dict), inputs.get_value_data("degree_property_name")
        )

        # isolated
        isolated_flag_dict = {id: True for id in nx.isolates(graph)}
        nx.set_node_attributes(
            graph, isolated_flag_dict, inputs.get_value_data("isolated_property_name")
        )

        if nx.is_directed(graph):
            graph = typing.cast(nx.DiGraph, graph)

            # indegree
            indegree_dict = graph.in_degree()
            nx.set_node_attributes(
                graph,
                dict(indegree_dict),
                inputs.get_value_data("indegree_property_name"),
            )

            # outdegree
            outdegree_dict = graph.out_degree()
            nx.set_node_attributes(
                graph,
                dict(outdegree_dict),
                inputs.get_value_data("outdegree_property_name"),
            )

        # eigenvector
        # betweenness
        betweenness_dict = nx.betweenness_centrality(graph)
        eigenvector_dict = nx.eigenvector_centrality(graph)

        nx.set_node_attributes(
            graph, betweenness_dict, inputs.get_value_data("betweenness_property_name")
        )
        nx.set_node_attributes(
            graph, eigenvector_dict, inputs.get_value_data("eigenvector_property_name")
        )

        outputs.set_value("graph", graph)


class GraphToNodesTableTransformationModule(KiaraModule):
    """Transform a network_graph object to an Arrow table with a list of nodes and their properties."""

    _module_type_name = 'to_nodes_table'

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        return {
            'source': ValueSchema(type='network_graph'),
            'node_id_column': ValueSchema(type='string', default='id')
        }

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        return {
            'target': ValueSchema(type='table')
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        import pyarrow as pa
        import pandas as pd

        graph: Graph = inputs.get_value_data('source')
        node_id_column: str = inputs.get_value_data('node_id_column')

        nodes = [
            {**node_attrs, node_id_column: node_id}
            for node_id, node_attrs in graph.nodes.data()
        ]
        df = pd.DataFrame.from_records(nodes)
        table = pa.Table.from_pandas(df)
        outputs.set_value('target', table)


class GraphToEdgesTableTransformationModule(KiaraModule):
    """Transform a network_graph object to an Arrow table with a list of edges."""
    _module_type_name = 'to_edges_table'

    def create_input_schema(self) -> typing.Mapping[str, ValueSchema]:
        return {
            'source': ValueSchema(type='network_graph')
        }

    def create_output_schema(self) -> typing.Mapping[str, ValueSchema]:
        return {
            'target': ValueSchema(type='table')
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        import pyarrow as pa

        graph: Graph = inputs.get_value_data('source')
        df = nx.to_pandas_edgelist(graph, "source", "target")
        table = pa.Table.from_pandas(df)
        outputs.set_value('target', table)

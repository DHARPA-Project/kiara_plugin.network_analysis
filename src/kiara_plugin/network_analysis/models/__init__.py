# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.network_analysis`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""

from typing import (
    TYPE_CHECKING,
    ClassVar,
    Dict,
    Iterable,
    List,
    Literal,
    Protocol,
    Set,
    Type,
    TypeVar,
    Union,
)

from pydantic import BaseModel, Field

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.network_analysis.defaults import (
    ATTRIBUTE_PROPERTY_KEY,
    COMPONENT_ID_COLUMN_NAME,
    CONNECTIONS_COLUMN_NAME,
    CONNECTIONS_MULTI_COLUMN_NAME,
    COUNT_DIRECTED_COLUMN_NAME,
    COUNT_IDX_DIRECTED_COLUMN_NAME,
    COUNT_IDX_UNDIRECTED_COLUMN_NAME,
    COUNT_UNDIRECTED_COLUMN_NAME,
    EDGE_ID_COLUMN_NAME,
    EDGES_TABLE_NAME,
    IN_DIRECTED_COLUMN_NAME,
    IN_DIRECTED_MULTI_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    NODE_ID_COLUMN_NAME,
    NODES_TABLE_NAME,
    OUT_DIRECTED_COLUMN_NAME,
    OUT_DIRECTED_MULTI_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
    UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME,
    UNWEIGHTED_DEGREE_CENTRALITY_MULTI_COLUMN_NAME,
    GraphType,
)
from kiara_plugin.network_analysis.utils import (
    augment_edges_table_with_id_and_weights,
    augment_nodes_table_with_connection_counts,
    augment_tables_with_component_ids,
    extract_networkx_edges_as_table,
    extract_networkx_nodes_as_table,
)
from kiara_plugin.tabular.models.tables import KiaraTables

if TYPE_CHECKING:
    import networkx as nx
    import pyarrow as pa
    import rustworkx as rx

    from kiara_plugin.network_analysis.models.metadata import (
        NetworkNodeAttributeMetadata,
    )
    from kiara_plugin.tabular.models.table import KiaraTable

NETWORKX_GRAPH_TYPE = TypeVar("NETWORKX_GRAPH_TYPE", bound="nx.Graph")
RUSTWORKX_GRAPH_TYPE = TypeVar("RUSTWORKX_GRAPH_TYPE", "rx.PyGraph", "rx.PyDiGraph")


class NodesCallback(Protocol):
    def __call__(self, _node_id: int, **kwargs) -> None: ...


class EdgesCallback(Protocol):
    def __call__(self, _source: int, _target: int, **kwargs) -> None: ...


class NetworkData(KiaraTables):
    """A flexible, graph-type agnostic wrapper class for network datasets.

    This class provides a unified interface for working with network data that can represent
    any type of graph structure: directed, undirected, simple, or multi-graphs. The design
    philosophy emphasizes flexibility and performance while maintaining a clean, intuitive API.

    **Design Philosophy:**
    - **Graph Type Agnostic**: Supports all graph types (directed/undirected, simple/multi)
      within the same data structure without requiring type-specific conversions
    - **Efficient Storage**: Uses Apache Arrow tables for high-performance columnar storage
    - **Flexible Querying**: Provides SQL-based querying capabilities alongside programmatic access
    - **Seamless Export**: Easy conversion to NetworkX and RustWorkX graph objects, other representations possible in the future
    - **Metadata Rich**: Automatically computes and stores graph statistics and properties

    **Internal Structure:**
    The network data is stored as two Arrow tables:
    - **nodes table**: Contains node information with required columns '_node_id' (int) and '_label' (str)
    - **edges table**: Contains edge information with required columns '_source' (int) and '_target' (int)

    Additional computed columns (prefixed with '_') provide graph statistics for different interpretations:
    - Degree counts for directed/undirected graphs
    - Multi-edge counts and indices
    - Centrality measures

    **Graph Type Support:**
    - **Simple Graphs**: Single edges between node pairs
    - **Multi-graphs**: Multiple edges between the same node pairs
    - **Directed Graphs**: One-way edges with source → target semantics
    - **Undirected Graphs**: Bidirectional edges
    - **Mixed Types**: The same data can be interpreted as different graph types

    **Note:** Column names prefixed with '_' have internal meaning and are automatically
    computed. Original attributes from source data are stored without the prefix.
    """

    _kiara_model_id: ClassVar = "instance.network_data"

    @classmethod
    def create_augmented(
        cls,
        network_data: "NetworkData",
        additional_edges_columns: Union[None, Dict[str, "pa.Array"]] = None,
        additional_nodes_columns: Union[None, Dict[str, "pa.Array"]] = None,
        nodes_column_metadata: Union[Dict[str, Dict[str, KiaraModel]], None] = None,
        edges_column_metadata: Union[Dict[str, Dict[str, KiaraModel]], None] = None,
    ) -> "NetworkData":
        """Create a new NetworkData instance with additional columns.

        This method creates a new NetworkData instance by adding extra columns to an existing
        instance without recomputing the automatically generated internal columns (those
        prefixed with '_'). This is useful for adding derived attributes or analysis results.

        Args:
            network_data: The source NetworkData instance to augment
            additional_edges_columns: Dictionary mapping column names to PyArrow Arrays
                for new edge attributes
            additional_nodes_columns: Dictionary mapping column names to PyArrow Arrays
                for new node attributes
            nodes_column_metadata: Additional metadata to attach to nodes table columns
            edges_column_metadata: Additional metadata to attach to edges table columns

        Returns:
            NetworkData: A new NetworkData instance with the additional columns

        Example:
            ```python
            import pyarrow as pa

            # Add a weight column to edges
            weights = pa.array([1.0, 2.5, 0.8] * (network_data.num_edges // 3))
            augmented = NetworkData.create_augmented(
                network_data,
                additional_edges_columns={"weight": weights}
            )
            ```
        """

        nodes_table = network_data.nodes.arrow_table
        edges_table = network_data.edges.arrow_table

        # nodes_table = pa.Table.from_arrays(orig_nodes_table.columns, schema=orig_nodes_table.schema)
        # edges_table = pa.Table.from_arrays(orig_edges_table.columns, schema=orig_edges_table.schema)

        if additional_edges_columns is not None:
            for col_name, col_data in additional_edges_columns.items():
                edges_table = edges_table.append_column(col_name, col_data)

        if additional_nodes_columns is not None:
            for col_name, col_data in additional_nodes_columns.items():
                nodes_table = nodes_table.append_column(col_name, col_data)

        new_network_data = NetworkData.create_network_data(
            nodes_table=nodes_table,
            edges_table=edges_table,
            augment_tables=False,
            nodes_column_metadata=nodes_column_metadata,
            edges_column_metadata=edges_column_metadata,
        )

        return new_network_data

    @classmethod
    def create_network_data(
        cls,
        nodes_table: "pa.Table",
        edges_table: "pa.Table",
        augment_tables: bool = True,
        nodes_column_metadata: Union[Dict[str, Dict[str, KiaraModel]], None] = None,
        edges_column_metadata: Union[Dict[str, Dict[str, KiaraModel]], None] = None,
    ) -> "NetworkData":
        """Create a NetworkData instance from PyArrow tables.

        This is the primary factory method for creating NetworkData instances from raw tabular data.
        It supports all graph types and automatically computes necessary metadata for efficient
        graph operations.

        **Required Table Structure:**

        Nodes table must contain:
        - '_node_id' (int): Unique integer identifier for each node
        - '_label' (str): Human-readable label for the node

        Edges table must contain:
        - '_source' (int): Source node ID (must exist in nodes table)
        - '_target' (int): Target node ID (must exist in nodes table)

        **Automatic Augmentation:**
        When `augment_tables=True` (default), the method automatically adds computed columns:

        For edges:
        - '_edge_id': Unique edge identifier
        - '_count_dup_directed': Count of parallel edges (directed interpretation)
        - '_idx_dup_directed': Index within parallel edge group (directed)
        - '_count_dup_undirected': Count of parallel edges (undirected interpretation)
        - '_idx_dup_undirected': Index within parallel edge group (undirected)

        For nodes:
        - '_count_edges': Total edge count (simple graph interpretation)
        - '_count_edges_multi': Total edge count (multi-graph interpretation)
        - '_in_edges': Incoming edge count (directed, simple)
        - '_out_edges': Outgoing edge count (directed, simple)
        - '_in_edges_multi': Incoming edge count (directed, multi)
        - '_out_edges_multi': Outgoing edge count (directed, multi)
        - '_degree_centrality': Normalized degree centrality
        - '_degree_centrality_multi': Normalized degree centrality (multi-graph)

        Args:
            nodes_table: PyArrow table containing node data
            edges_table: PyArrow table containing edge data
            augment_tables: Whether to compute and add internal metadata columns.
                Set to False only if you know the metadata is already present and correct.
            nodes_column_metadata: Additional metadata to attach to nodes table columns.
                Format: {column_name: {property_name: property_value}}
            edges_column_metadata: Additional metadata to attach to edges table columns.
                Format: {column_name: {property_name: property_value}}

        Returns:
            NetworkData: A new NetworkData instance

        Raises:
            KiaraException: If required columns are missing or contain null values

        """

        from kiara_plugin.network_analysis.models.metadata import (
            EDGE_COUNT_DUP_DIRECTED_COLUMN_METADATA,
            EDGE_COUNT_DUP_UNDIRECTED_COLUMN_METADATA,
            EDGE_ID_COLUMN_METADATA,
            EDGE_IDX_DUP_DIRECTED_COLUMN_METADATA,
            EDGE_IDX_DUP_UNDIRECTED_COLUMN_METADATA,
            EDGE_SOURCE_COLUMN_METADATA,
            EDGE_TARGET_COLUMN_METADATA,
            NODE_COUND_EDGES_MULTI_COLUMN_METADATA,
            NODE_COUNT_EDGES_COLUMN_METADATA,
            NODE_COUNT_IN_EDGES_COLUMN_METADATA,
            NODE_COUNT_IN_EDGES_MULTI_COLUMN_METADATA,
            NODE_COUNT_OUT_EDGES_COLUMN_METADATA,
            NODE_COUNT_OUT_EDGES_MULTI_COLUMN_METADATA,
            NODE_DEGREE_COLUMN_METADATA,
            NODE_DEGREE_MULTI_COLUMN_METADATA,
            NODE_ID_COLUMN_METADATA,
            NODE_LABEL_COLUMN_METADATA,
        )

        if augment_tables:
            edges_table = augment_edges_table_with_id_and_weights(edges_table)
            nodes_table = augment_nodes_table_with_connection_counts(
                nodes_table, edges_table
            )
            nodes_table, edges_table = augment_tables_with_component_ids(
                nodes_table=nodes_table, edges_table=edges_table
            )

        if edges_table.column(SOURCE_COLUMN_NAME).null_count > 0:
            raise KiaraException(
                msg="Can't assemble network data.",
                details="Source column in edges table contains null values.",
            )
        if edges_table.column(TARGET_COLUMN_NAME).null_count > 0:
            raise KiaraException(
                msg="Can't assemble network data.",
                details="Target column in edges table contains null values.",
            )

        network_data: NetworkData = cls.create_tables(
            {NODES_TABLE_NAME: nodes_table, EDGES_TABLE_NAME: edges_table}
        )

        # set default column metadata
        network_data.edges.set_column_metadata(
            EDGE_ID_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_ID_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            SOURCE_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_SOURCE_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            TARGET_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_TARGET_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_COUNT_DUP_DIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_IDX_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_IDX_DUP_DIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_UNDIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_COUNT_DUP_UNDIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.edges.set_column_metadata(
            COUNT_IDX_UNDIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            EDGE_IDX_DUP_UNDIRECTED_COLUMN_METADATA,
            overwrite_existing=False,
        )

        network_data.nodes.set_column_metadata(
            NODE_ID_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_ID_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            LABEL_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_LABEL_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            CONNECTIONS_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_EDGES_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            UNWEIGHTED_DEGREE_CENTRALITY_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_DEGREE_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            CONNECTIONS_MULTI_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUND_EDGES_MULTI_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            UNWEIGHTED_DEGREE_CENTRALITY_MULTI_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_DEGREE_MULTI_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            IN_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_IN_EDGES_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            IN_DIRECTED_MULTI_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_IN_EDGES_MULTI_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            OUT_DIRECTED_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_OUT_EDGES_COLUMN_METADATA,
            overwrite_existing=False,
        )
        network_data.nodes.set_column_metadata(
            OUT_DIRECTED_MULTI_COLUMN_NAME,
            ATTRIBUTE_PROPERTY_KEY,
            NODE_COUNT_OUT_EDGES_MULTI_COLUMN_METADATA,
            overwrite_existing=False,
        )

        if nodes_column_metadata is not None:
            for col_name, col_meta in nodes_column_metadata.items():
                for prop_name, prop_value in col_meta.items():
                    network_data.nodes.set_column_metadata(
                        col_name, prop_name, prop_value, overwrite_existing=True
                    )
        if edges_column_metadata is not None:
            for col_name, col_meta in edges_column_metadata.items():
                for prop_name, prop_value in col_meta.items():
                    network_data.edges.set_column_metadata(
                        col_name, prop_name, prop_value, overwrite_existing=True
                    )

        return network_data

    @classmethod
    def from_filtered_nodes(
        cls, network_data: "NetworkData", nodes_list: List[int]
    ) -> "NetworkData":
        """Create a new, filtered instance of this class using a source network, and a list of node ids to include.

        Nodes/edges containing a node id not in the list will be removed from the resulting network data.

        Arguments:
            network_data: the source network data
            nodes_list: the list of node ids to include in the filtered network data
        """

        import duckdb
        import polars as pl

        node_columns = [NODE_ID_COLUMN_NAME, LABEL_COLUMN_NAME]
        for column_name, metadata in network_data.nodes.column_metadata.items():
            attr_prop: Union[None, NetworkNodeAttributeMetadata] = metadata.get(  # type: ignore
                ATTRIBUTE_PROPERTY_KEY, None
            )
            if attr_prop is None or not attr_prop.computed_attribute:
                node_columns.append(column_name)

        node_list_str = ", ".join([str(n) for n in nodes_list])

        nodes_table = network_data.nodes.arrow_table  # noqa
        nodes_query = f"SELECT {', '.join(node_columns)} FROM nodes_table n WHERE n.{NODE_ID_COLUMN_NAME} IN ({node_list_str})"

        nodes_result = duckdb.sql(nodes_query).pl()

        edges_table = network_data.edges.arrow_table  # noqa
        edge_columns = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        for column_name, metadata in network_data.edges.column_metadata.items():
            attr_prop = metadata.get(ATTRIBUTE_PROPERTY_KEY, None)  # type: ignore
            if attr_prop is None or not attr_prop.computed_attribute:
                edge_columns.append(column_name)

        edges_query = f"SELECT {', '.join(edge_columns)} FROM edges_table WHERE {SOURCE_COLUMN_NAME} IN ({node_list_str}) OR {TARGET_COLUMN_NAME} IN ({node_list_str})"

        edges_result = duckdb.sql(edges_query).pl()

        nodes_idx_colum = range(len(nodes_result))
        old_idx_column = nodes_result[NODE_ID_COLUMN_NAME]

        repl_map = dict(zip(old_idx_column.to_list(), nodes_idx_colum))
        nodes_result = nodes_result.with_columns(
            pl.col(NODE_ID_COLUMN_NAME).replace_strict(repl_map, default=None)
        )

        edges_result = edges_result.with_columns(
            pl.col(SOURCE_COLUMN_NAME).replace_strict(repl_map, default=None),
            pl.col(TARGET_COLUMN_NAME).replace_strict(repl_map, default=None),
        )

        filtered = NetworkData.create_network_data(
            nodes_table=nodes_result, edges_table=edges_result
        )
        return filtered

    @classmethod
    def create_from_networkx_graph(
        cls,
        graph: "nx.Graph",
        label_attr_name: Union[str, None] = None,
        ignore_node_attributes: Union[Iterable[str], None] = None,
    ) -> "NetworkData":
        """Create a NetworkData instance from any NetworkX graph type.

        This method provides seamless conversion from NetworkX graphs to NetworkData,
        preserving all node and edge attributes while automatically handling different
        graph types (Graph, DiGraph, MultiGraph, MultiDiGraph).

        **Graph Type Support:**
        - **nx.Graph**: Converted to undirected simple graph representation
        - **nx.DiGraph**: Converted to directed simple graph representation
        - **nx.MultiGraph**: Converted with multi-edge support (undirected)
        - **nx.MultiDiGraph**: Converted with multi-edge support (directed)

        **Attribute Handling:**
        All NetworkX node and edge attributes are preserved as columns in the resulting
        tables, except those starting with '_' (reserved for internal use).

        Args:
            graph: Any NetworkX graph instance (Graph, DiGraph, MultiGraph, MultiDiGraph)
            label_attr_name: Name of the node attribute to use as the node label.
                If None, the node ID is converted to string and used as label.
                Can also be an iterable of attribute names to try in order.
            ignore_node_attributes: List of node attribute names to exclude from
                the resulting nodes table

        Returns:
            NetworkData: A new NetworkData instance representing the graph

        Raises:
            KiaraException: If node/edge attributes contain names starting with '_'

        Note:
            Node IDs in the original NetworkX graph are mapped to sequential integers
            starting from 0 in the NetworkData representation. The original node IDs
            are preserved as the '_label' if no label_attr_name is specified.
        """

        # TODO: should we also index nodes/edges attributes?

        nodes_table, node_id_map = extract_networkx_nodes_as_table(
            graph=graph,
            label_attr_name=label_attr_name,
            ignore_attributes=ignore_node_attributes,
        )

        edges_table = extract_networkx_edges_as_table(graph, node_id_map)

        network_data = NetworkData.create_network_data(
            nodes_table=nodes_table, edges_table=edges_table
        )

        return network_data

    @property
    def edges(self) -> "KiaraTable":
        """Access the edges table containing all edge data and computed statistics.

        The edges table contains both original edge attributes and computed columns:
        - '_edge_id': Unique edge identifier
        - '_source', '_target': Node IDs for edge endpoints
        - '_count_dup_*': Parallel edge counts for different graph interpretations
        - '_idx_dup_*': Indices within parallel edge groups
        - Original edge attributes (without '_' prefix)

        Returns:
            KiaraTable: The edges table with full schema and data access methods
        """
        return self.tables[EDGES_TABLE_NAME]

    @property
    def nodes(self) -> "KiaraTable":
        """Access the nodes table containing all node data and computed statistics.

        The nodes table contains both original node attributes and computed columns:
        - '_node_id': Unique node identifier (sequential integers from 0)
        - '_label': Human-readable node label
        - '_count_edges*': Edge counts for different graph interpretations
        - '_in_edges*', '_out_edges*': Directional edge counts
        - '_degree_centrality*': Normalized degree centrality measures
        - Original node attributes (without '_' prefix)

        Returns:
            KiaraTable: The nodes table with full schema and data access methods
        """
        return self.tables[NODES_TABLE_NAME]

    @property
    def num_nodes(self) -> int:
        """Get the total number of nodes in the network.

        Returns:
            int: Number of nodes in the network
        """
        return self.nodes.num_rows  # type: ignore

    @property
    def num_edges(self) -> int:
        """Get the total number of edges in the network.

        Note: This returns the total number of edge records, which includes
        all parallel edges in multi-graph interpretations.

        Returns:
            int: Total number of edges (including parallel edges)
        """
        return self.edges.num_rows  # type: ignore

    def query_edges(
        self, sql_query: str, relation_name: str = EDGES_TABLE_NAME
    ) -> "pa.Table":
        """Execute SQL queries on the edges table for flexible data analysis.

        This method provides direct SQL access to the edges table, enabling complex
        queries and aggregations. All computed edge columns are available for querying.

        **Available Columns:**
        - '_edge_id': Unique edge identifier
        - '_source', '_target': Node IDs for edge endpoints
        - '_count_dup_directed': Number of parallel edges (directed interpretation)
        - '_idx_dup_directed': Index within parallel edge group (directed)
        - '_count_dup_undirected': Number of parallel edges (undirected interpretation)
        - '_idx_dup_undirected': Index within parallel edge group (undirected)
        - Original edge attributes (names without '_' prefix)

        Args:
            sql_query: SQL query string. Use 'edges' as the table name in your query.
            relation_name: Alternative table name to use in the query (default: 'edges').
                If specified, all occurrences of this name in the query will be replaced
                with 'edges'.

        Returns:
            pa.Table: Query results as a PyArrow table

        Example:
            ```python
            # Find edges with high multiplicity
            parallel_edges = network_data.query_edges(
                "SELECT _source, _target, _count_dup_directed FROM edges WHERE _count_dup_directed > 1"
            )

            # Get edge statistics
            stats = network_data.query_edges(
                "SELECT COUNT(*) as total_edges, AVG(_count_dup_directed) as avg_multiplicity FROM edges"
            )
            ```
        """
        import duckdb

        con = duckdb.connect()
        edges = self.edges.arrow_table  # noqa: F841
        if relation_name != EDGES_TABLE_NAME:
            sql_query = sql_query.replace(relation_name, EDGES_TABLE_NAME)

        result = con.execute(sql_query)
        return result.arrow()

    def query_nodes(
        self, sql_query: str, relation_name: str = NODES_TABLE_NAME
    ) -> "pa.Table":
        """Execute SQL queries on the nodes table for flexible data analysis.

        This method provides direct SQL access to the nodes table, enabling complex
        queries and aggregations. All computed node statistics are available for querying.

        **Available Columns:**
        - '_node_id': Unique node identifier
        - '_label': Human-readable node label
        - '_count_edges': Total edge count (simple graph interpretation)
        - '_count_edges_multi': Total edge count (multi-graph interpretation)
        - '_in_edges': Incoming edge count (directed, simple)
        - '_out_edges': Outgoing edge count (directed, simple)
        - '_in_edges_multi': Incoming edge count (directed, multi)
        - '_out_edges_multi': Outgoing edge count (directed, multi)
        - '_degree_centrality': Normalized degree centrality (simple)
        - '_degree_centrality_multi': Normalized degree centrality (multi)
        - Original node attributes (names without '_' prefix)

        Args:
            sql_query: SQL query string. Use 'nodes' as the table name in your query.
            relation_name: Alternative table name to use in the query (default: 'nodes').
                If specified, all occurrences of this name in the query will be replaced
                with 'nodes'.

        Returns:
            pa.Table: Query results as a PyArrow table

        Example:
            ```python
            # Find high-degree nodes
            hubs = network_data.query_nodes(
                "SELECT _node_id, _label, _count_edges FROM nodes WHERE _count_edges > 10 ORDER BY _count_edges DESC"
            )

            # Get centrality statistics
            centrality_stats = network_data.query_nodes(
                "SELECT AVG(_degree_centrality) as avg_centrality, MAX(_degree_centrality) as max_centrality FROM nodes"
            )
            ```
        """
        import duckdb

        con = duckdb.connect()
        nodes = self.nodes.arrow_table  # noqa
        if relation_name != NODES_TABLE_NAME:
            sql_query = sql_query.replace(relation_name, NODES_TABLE_NAME)

        result = con.execute(sql_query)
        return result.arrow()

    def _calculate_node_attributes(
        self, incl_node_attributes: Union[bool, str, Iterable[str]]
    ) -> List[str]:
        """Calculate the node attributes that should be included in the output."""

        if incl_node_attributes is False:
            node_attr_names: List[str] = [NODE_ID_COLUMN_NAME, LABEL_COLUMN_NAME]
        else:
            all_node_attr_names: List[str] = self.nodes.column_names  # type: ignore
            if incl_node_attributes is True:
                node_attr_names = [NODE_ID_COLUMN_NAME]
                node_attr_names.extend(
                    (x for x in all_node_attr_names if x != NODE_ID_COLUMN_NAME)
                )  # type: ignore
            elif isinstance(incl_node_attributes, str):
                if incl_node_attributes not in all_node_attr_names:
                    raise KiaraException(
                        f"Can't include node attribute {incl_node_attributes}: not part of the available attributes ({', '.join(all_node_attr_names)})."
                    )
                node_attr_names = [NODE_ID_COLUMN_NAME, incl_node_attributes]
            else:
                node_attr_names = [NODE_ID_COLUMN_NAME]
                for attr_name in incl_node_attributes:
                    if attr_name not in all_node_attr_names:
                        raise KiaraException(
                            f"Can't include node attribute {incl_node_attributes}: not part of the available attributes ({', '.join(all_node_attr_names)})."
                        )
                    node_attr_names.append(attr_name)  # type: ignore

        return node_attr_names

    def _calculate_edge_attributes(
        self, incl_edge_attributes: Union[bool, str, Iterable[str]]
    ) -> List[str]:
        """Calculate the edge attributes that should be included in the output."""

        if incl_edge_attributes is False:
            edge_attr_names: List[str] = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
        else:
            all_edge_attr_names: List[str] = self.edges.column_names  # type: ignore
            if incl_edge_attributes is True:
                edge_attr_names = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
                edge_attr_names.extend(
                    (
                        x
                        for x in all_edge_attr_names
                        if x not in (SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME)
                    )
                )  # type: ignore
            elif isinstance(incl_edge_attributes, str):
                if incl_edge_attributes not in all_edge_attr_names:
                    raise KiaraException(
                        f"Can't include edge attribute {incl_edge_attributes}: not part of the available attributes ({', '.join(all_edge_attr_names)})."
                    )
                edge_attr_names = [
                    SOURCE_COLUMN_NAME,
                    TARGET_COLUMN_NAME,
                    incl_edge_attributes,
                ]
            else:
                edge_attr_names = [SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME]
                for attr_name in incl_edge_attributes:
                    if attr_name not in all_edge_attr_names:
                        raise KiaraException(
                            f"Can't include edge attribute {incl_edge_attributes}: not part of the available attributes ({', '.join(all_edge_attr_names)})."
                        )
                    edge_attr_names.append(attr_name)  # type: ignore

        return edge_attr_names

    def retrieve_graph_data(
        self,
        nodes_callback: Union[NodesCallback, None] = None,
        edges_callback: Union[EdgesCallback, None] = None,
        incl_node_attributes: Union[bool, str, Iterable[str]] = False,
        incl_edge_attributes: Union[bool, str, Iterable[str]] = False,
        omit_self_loops: bool = False,
    ):
        """Retrieve graph data from the sqlite database, and call the specified callbacks for each node and edge.

        First the nodes will be processed, then the edges, if that does not suit your needs you can just use this method twice, and set the callback you don't need to None.

        The nodes_callback will be called with the following arguments:
            - node_id: the id of the node (int)
            - if False: nothing else
            - if True: all node attributes, in the order they are defined in the table schema
            - if str: the value of the specified node attribute
            - if Iterable[str]: the values of the specified node attributes, in the order they are specified

        The edges_callback will be called with the following aruments:
            - source_id: the id of the source node (int)
            - target_id: the id of the target node (int)
            - if False: nothing else
            - if True: all edge attributes, in the order they are defined in the table schema
            - if str: the value of the specified edge attribute
            - if Iterable[str]: the values of the specified edge attributes, in the order they are specified

        """

        if nodes_callback is not None:
            node_attr_names = self._calculate_node_attributes(incl_node_attributes)

            nodes_df = self.nodes.to_polars_dataframe()
            for row in nodes_df.select(*node_attr_names).rows(named=True):
                nodes_callback(**row)  # type: ignore

        if edges_callback is not None:
            edge_attr_names = self._calculate_edge_attributes(incl_edge_attributes)

            edges_df = self.edges.to_polars_dataframe()
            for row in edges_df.select(*edge_attr_names).rows(named=True):
                if (
                    omit_self_loops
                    and row[SOURCE_COLUMN_NAME] == row[TARGET_COLUMN_NAME]
                ):
                    continue
                edges_callback(**row)  # type: ignore

    def as_networkx_graph(
        self,
        graph_type: Type[NETWORKX_GRAPH_TYPE],
        incl_node_attributes: Union[bool, str, Iterable[str]] = False,
        incl_edge_attributes: Union[bool, str, Iterable[str]] = False,
        omit_self_loops: bool = False,
    ) -> NETWORKX_GRAPH_TYPE:
        """Export the network data as a NetworkX graph object.

        This method converts the NetworkData to any NetworkX graph type, providing
        flexibility to work with the data using NetworkX's extensive algorithm library.
        The conversion preserves node and edge attributes as specified.

        **Supported Graph Types:**
        - **nx.Graph**: Undirected simple graph (parallel edges are merged)
        - **nx.DiGraph**: Directed simple graph (parallel edges are merged)
        - **nx.MultiGraph**: Undirected multigraph (parallel edges preserved)
        - **nx.MultiDiGraph**: Directed multigraph (parallel edges preserved)

        **Attribute Handling:**
        Node and edge attributes can be selectively included in the exported graph.
        Internal columns (prefixed with '_') are available but typically excluded
        from exports to maintain clean NetworkX compatibility.

        Args:
            graph_type: NetworkX graph class to instantiate (nx.Graph, nx.DiGraph, etc.)
            incl_node_attributes: Controls which node attributes to include:
                - False: No attributes (only node IDs)
                - True: All attributes (including computed columns)
                - str: Single attribute name to include
                - Iterable[str]: List of specific attributes to include
            incl_edge_attributes: Controls which edge attributes to include:
                - False: No attributes
                - True: All attributes (including computed columns)
                - str: Single attribute name to include
                - Iterable[str]: List of specific attributes to include
            omit_self_loops: If True, edges where source equals target are excluded

        Returns:
            NETWORKX_GRAPH_TYPE: NetworkX graph instance of the specified type

        Note:
            When exporting to simple graph types (Graph, DiGraph), parallel edges
            are automatically merged. Use MultiGraph or MultiDiGraph to preserve
            all edge instances.
        """

        graph: NETWORKX_GRAPH_TYPE = graph_type()

        def add_node(_node_id: int, **attrs):
            graph.add_node(_node_id, **attrs)

        def add_edge(_source: int, _target: int, **attrs):
            graph.add_edge(_source, _target, **attrs)

        self.retrieve_graph_data(
            nodes_callback=add_node,
            edges_callback=add_edge,
            incl_node_attributes=incl_node_attributes,
            incl_edge_attributes=incl_edge_attributes,
            omit_self_loops=omit_self_loops,
        )

        return graph

    def as_rustworkx_graph(
        self,
        graph_type: Type[RUSTWORKX_GRAPH_TYPE],
        multigraph: bool = False,
        incl_node_attributes: Union[bool, str, Iterable[str]] = False,
        incl_edge_attributes: Union[bool, str, Iterable[str]] = False,
        omit_self_loops: bool = False,
        attach_node_id_map: bool = False,
    ) -> RUSTWORKX_GRAPH_TYPE:
        """Export the network data as a RustWorkX graph object.

        RustWorkX provides high-performance graph algorithms implemented in Rust with
        Python bindings. This method converts NetworkData to RustWorkX format while
        handling the differences in node ID management between the two systems.

        **Supported Graph Types:**
        - **rx.PyGraph**: Undirected graph (with optional multigraph support)
        - **rx.PyDiGraph**: Directed graph (with optional multigraph support)

        **Node ID Mapping:**
        RustWorkX uses sequential integer node IDs starting from 0, which may differ
        from the original NetworkData node IDs. The original '_node_id' values are
        preserved as node attributes, and an optional mapping can be attached to
        the graph for reference.

        **Performance Benefits:**
        RustWorkX graphs offer significant performance advantages for:
        - Large-scale graph algorithms
        - Parallel processing
        - Memory-efficient operations
        - High-performance centrality calculations

        Args:
            graph_type: RustWorkX graph class (rx.PyGraph or rx.PyDiGraph)
            multigraph: If True, parallel edges are preserved; if False, they are merged
            incl_node_attributes: Controls which node attributes to include:
                - False: No attributes (only node data structure)
                - True: All attributes (including computed columns)
                - str: Single attribute name to include
                - Iterable[str]: List of specific attributes to include
            incl_edge_attributes: Controls which edge attributes to include:
                - False: No attributes
                - True: All attributes (including computed columns)
                - str: Single attribute name to include
                - Iterable[str]: List of specific attributes to include
            omit_self_loops: If True, self-loops (edges where source == target) are excluded
            attach_node_id_map: If True, adds a 'node_id_map' attribute to the graph
                containing the mapping from RustWorkX node IDs to original NetworkData node IDs

        Returns:
            RUSTWORKX_GRAPH_TYPE: RustWorkX graph instance of the specified type

        Note:
            The original NetworkData '_node_id' values are always included in the
            node data dictionary, regardless of the incl_node_attributes setting.
        """

        from bidict import bidict

        graph = graph_type(multigraph=multigraph)

        # rustworkx uses 0-based integer indexes, so we don't neeed to look up the node ids (unless we want to
        # include node attributes)

        self._calculate_node_attributes(incl_node_attributes)[1:]
        self._calculate_edge_attributes(incl_edge_attributes)[2:]

        # we can use a 'global' dict here because we know the nodes are processed before the edges
        node_map: bidict = bidict()

        def add_node(_node_id: int, **attrs):
            data = {NODE_ID_COLUMN_NAME: _node_id}
            data.update(attrs)

            graph_node_id = graph.add_node(data)

            node_map[graph_node_id] = _node_id
            # if not _node_id == graph_node_id:
            #     raise Exception("Internal error: node ids don't match")

        def add_edge(_source: int, _target: int, **attrs):
            source = node_map[_source]
            target = node_map[_target]
            if not attrs:
                graph.add_edge(source, target, None)
            else:
                graph.add_edge(source, target, attrs)

        self.retrieve_graph_data(
            nodes_callback=add_node,
            edges_callback=add_edge,
            incl_node_attributes=incl_node_attributes,
            incl_edge_attributes=incl_edge_attributes,
            omit_self_loops=omit_self_loops,
        )

        if attach_node_id_map:
            graph.attrs = {"node_id_map": node_map}  # type: ignore

        return graph

    @property
    def component_ids(self) -> Set[int]:
        import duckdb

        nodes_table = self.nodes.arrow_table  # noqa
        query = f"""
        SELECT DISTINCT {COMPONENT_ID_COLUMN_NAME} FROM nodes_table
        """

        result: Set[int] = {(x[0] for x in duckdb.sql(query).fetchall())}  # type: ignore
        return result


class GraphProperties(BaseModel):
    """Properties of graph data, if interpreted as a specific graph type."""

    number_of_edges: int = Field(description="The number of edges.")
    parallel_edges: int = Field(
        description="The number of parallel edges (if 'multi' graph type).", default=0
    )


class ComponentProperties(BaseModel):
    """Properties of a connected component."""

    component_id: int = Field(description="The id of the component.")
    number_of_nodes: int = Field(description="The number of nodes in the component.")
    number_of_associated_edge_rows: int = Field(
        description="The number of edge rows associated to the component."
    )


class NetworkGraphProperties(ValueMetadata):
    """Network data stats."""

    _metadata_key: ClassVar[str] = "network_data"

    number_of_nodes: int = Field(description="Number of nodes in the network graph.")
    properties_by_graph_type: Dict[  # type: ignore
        Literal[
            GraphType.DIRECTED.value,
            GraphType.UNDIRECTED.value,
            GraphType.UNDIRECTED_MULTI.value,
            GraphType.DIRECTED_MULTI.value,
        ],
        GraphProperties,
    ] = Field(description="Properties of the network data, by graph type.")
    number_of_self_loops: int = Field(
        description="Number of edges where source and target point to the same node."
    )
    number_of_components: int = Field(
        description="Number of connected components in the network graph."
    )
    components: Dict[int, ComponentProperties] = Field(
        description="Properties of the components of the network graph."
    )

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["network_data"]

    @classmethod
    def create_value_metadata(cls, value: Value) -> "NetworkGraphProperties":
        import duckdb

        network_data: NetworkData = value.data

        num_rows = network_data.num_nodes
        num_edges = network_data.num_edges

        # query_num_edges_directed = f"SELECT COUNT(*) FROM (SELECT DISTINCT {SOURCE_COLUMN_NAME}, {TARGET_COLUMN_NAME} FROM {EDGES_TABLE_NAME})"
        query_num_edges_directed = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_DIRECTED_COLUMN_NAME} = 1"

        num_edges_directed_result = network_data.query_edges(query_num_edges_directed)
        num_edges_directed = num_edges_directed_result.columns[0][0].as_py()

        query_num_edges_undirected = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_UNDIRECTED_COLUMN_NAME} = 1"
        num_edges_undirected_result = network_data.query_edges(
            query_num_edges_undirected
        )
        num_edges_undirected = num_edges_undirected_result.columns[0][0].as_py()

        self_loop_query = f"SELECT count(*) FROM {EDGES_TABLE_NAME} WHERE {SOURCE_COLUMN_NAME} = {TARGET_COLUMN_NAME}"
        self_loop_result = network_data.query_edges(self_loop_query)
        num_self_loops = self_loop_result.columns[0][0].as_py()

        num_parallel_edges_directed_query = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_DIRECTED_COLUMN_NAME} = 2"
        num_parallel_edges_directed_result = network_data.query_edges(
            num_parallel_edges_directed_query
        )
        num_parallel_edges_directed = num_parallel_edges_directed_result.columns[0][
            0
        ].as_py()

        num_parallel_edges_undirected_query = f"SELECT COUNT(*) FROM {EDGES_TABLE_NAME} WHERE {COUNT_IDX_UNDIRECTED_COLUMN_NAME} = 2"
        num_parallel_edges_undirected_result = network_data.query_edges(
            num_parallel_edges_undirected_query
        )
        num_parallel_edges_undirected = num_parallel_edges_undirected_result.columns[0][
            0
        ].as_py()

        directed_props = GraphProperties(number_of_edges=num_edges_directed)
        undirected_props = GraphProperties(number_of_edges=num_edges_undirected)
        directed_multi_props = GraphProperties(
            number_of_edges=num_edges, parallel_edges=num_parallel_edges_directed
        )
        undirected_multi_props = GraphProperties(
            number_of_edges=num_edges, parallel_edges=num_parallel_edges_undirected
        )

        props = {
            GraphType.DIRECTED.value: directed_props,
            GraphType.DIRECTED_MULTI.value: directed_multi_props,
            GraphType.UNDIRECTED.value: undirected_props,
            GraphType.UNDIRECTED_MULTI.value: undirected_multi_props,
        }

        nodes_table = network_data.nodes.arrow_table  # noqa
        edges_table = network_data.edges.arrow_table  # noqa

        components_query_nodes = f"""
            SELECT
                {COMPONENT_ID_COLUMN_NAME}, COUNT(*)
            FROM
                nodes_table
            GROUP BY {COMPONENT_ID_COLUMN_NAME}
        """
        nodes_result = duckdb.query(components_query_nodes)
        nodes_data = nodes_result.fetchall()
        nodes_data = {row[0]: row[1] for row in nodes_data}

        components_query_edges = f"""
            SELECT
                {COMPONENT_ID_COLUMN_NAME}, COUNT(*)
            FROM
                edges_table
            GROUP BY {COMPONENT_ID_COLUMN_NAME}
        """
        edges_result = duckdb.query(components_query_edges)
        edges_data = edges_result.fetchall()
        edges_data = {row[0]: row[1] for row in edges_data}

        components_data = {}
        for component_id, num_nodes in nodes_data.items():
            num_edges = edges_data.get(component_id, 0)
            components_data[component_id] = ComponentProperties(
                component_id=component_id,
                number_of_nodes=num_nodes,
                number_of_associated_edge_rows=num_edges,
            )

        number_of_components = len(components_data)

        result = cls(
            number_of_nodes=num_rows,
            properties_by_graph_type=props,
            number_of_self_loops=num_self_loops,
            number_of_components=number_of_components,
            components={k: components_data[k] for k in sorted(components_data.keys())},
        )
        return result

# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_plugin.network_analysis`` package.
"""
from typing import Any, ClassVar, List, Mapping, Type, Union

from rich.console import Group

from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.exceptions import KiaraException
from kiara.models.values.value import Value
from kiara.utils.output import ArrowTabularWrap
from kiara_plugin.network_analysis.defaults import (
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
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.tabular.data_types.tables import TablesType
from kiara_plugin.tabular.models.tables import KiaraTables


class NetworkDataType(TablesType):
    """Data that can be assembled into a graph.

    This data type extends the 'tables' type from the [kiara_plugin.tabular](https://github.com/DHARPA-Project/kiara_plugin.tabular) plugin, restricting the allowed tables to one called 'edges',
    and one called 'nodes'.
    """

    _data_type_name: ClassVar[str] = "network_data"
    _cached_doc: ClassVar[Union[str, None]] = None

    @classmethod
    def python_class(cls) -> Type:
        return NetworkData  # type: ignore

    @classmethod
    def type_doc(cls) -> str:

        if cls._cached_doc:
            return cls._cached_doc

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
            NODE_ID_COLUMN_METADATA,
            NODE_LABEL_COLUMN_METADATA,
        )

        edge_properties = {}
        edge_properties[EDGE_ID_COLUMN_NAME] = EDGE_ID_COLUMN_METADATA.doc.full_doc
        edge_properties[SOURCE_COLUMN_NAME] = EDGE_SOURCE_COLUMN_METADATA.doc.full_doc
        edge_properties[TARGET_COLUMN_NAME] = EDGE_TARGET_COLUMN_METADATA.doc.full_doc
        edge_properties[
            COUNT_DIRECTED_COLUMN_NAME
        ] = EDGE_COUNT_DUP_DIRECTED_COLUMN_METADATA.doc.full_doc
        edge_properties[
            COUNT_IDX_DIRECTED_COLUMN_NAME
        ] = EDGE_IDX_DUP_DIRECTED_COLUMN_METADATA.doc.full_doc
        edge_properties[
            COUNT_UNDIRECTED_COLUMN_NAME
        ] = EDGE_COUNT_DUP_UNDIRECTED_COLUMN_METADATA.doc.full_doc
        edge_properties[
            COUNT_IDX_UNDIRECTED_COLUMN_NAME
        ] = EDGE_IDX_DUP_UNDIRECTED_COLUMN_METADATA.doc.full_doc

        properties_node = {}
        properties_node[NODE_ID_COLUMN_NAME] = NODE_ID_COLUMN_METADATA.doc.full_doc
        properties_node[LABEL_COLUMN_NAME] = NODE_LABEL_COLUMN_METADATA.doc.full_doc
        properties_node[
            CONNECTIONS_COLUMN_NAME
        ] = NODE_COUNT_EDGES_COLUMN_METADATA.doc.full_doc
        properties_node[
            CONNECTIONS_MULTI_COLUMN_NAME
        ] = NODE_COUND_EDGES_MULTI_COLUMN_METADATA.doc.full_doc
        properties_node[
            IN_DIRECTED_COLUMN_NAME
        ] = NODE_COUNT_IN_EDGES_COLUMN_METADATA.doc.full_doc
        properties_node[
            IN_DIRECTED_MULTI_COLUMN_NAME
        ] = NODE_COUNT_IN_EDGES_MULTI_COLUMN_METADATA.doc.full_doc
        properties_node[
            OUT_DIRECTED_COLUMN_NAME
        ] = NODE_COUNT_OUT_EDGES_COLUMN_METADATA.doc.full_doc
        properties_node[
            OUT_DIRECTED_MULTI_COLUMN_NAME
        ] = NODE_COUNT_OUT_EDGES_MULTI_COLUMN_METADATA.doc.full_doc

        edge_properties_str = "\n\n".join(
            f"***{key}***:\n\n{value}" for key, value in edge_properties.items()
        )
        node_properties_str = "\n\n".join(
            f"***{key}***:\n\n{value}" for key, value in properties_node.items()
        )

        doc = cls.__doc__
        doc_tables = f"""

## Edges
The 'edges' table contains the following columns:

{edge_properties_str}

## Nodes

The 'nodes' table contains the following columns:

{node_properties_str}

"""

        cls._cached_doc = f"{doc}\n\n{doc_tables}"
        return cls._cached_doc

    def parse_python_obj(self, data: Any) -> NetworkData:

        if isinstance(data, KiaraTables):
            if EDGES_TABLE_NAME not in data.tables.keys():
                raise KiaraException(
                    f"Can't import network data: no '{EDGES_TABLE_NAME}' table found"
                )

            if NODES_TABLE_NAME not in data.tables.keys():
                raise KiaraException(
                    f"Can't import network data: no '{NODES_TABLE_NAME}' table found"
                )

            # return NetworkData(
            #     tables={
            #         EDGES_TABLE_NAME: data.tables[EDGES_TABLE_NAME],
            #         NODES_TABLE_NAME: data.tables[NODES_TABLE_NAME],
            #     },
            #
            # )
            return NetworkData.create_network_data(
                edges_table=data.tables[EDGES_TABLE_NAME].arrow_table,
                nodes_table=data.tables[NODES_TABLE_NAME].arrow_table,
                augment_tables=False,
            )

        if not isinstance(data, NetworkData):
            raise KiaraException(
                f"Can't parse object to network data: invalid type '{type(data)}'."
            )

        return data

    def _validate(cls, value: Any) -> None:
        if not isinstance(value, NetworkData):
            raise ValueError(
                f"Invalid type '{type(value)}': must be of 'NetworkData' (or a sub-class)."
            )

        network_data: NetworkData = value

        table_names = network_data.table_names
        if EDGES_TABLE_NAME not in table_names:
            raise Exception(
                f"Invalid 'network_data' value: database does not contain table '{EDGES_TABLE_NAME}'."
            )
        if NODES_TABLE_NAME not in table_names:
            raise Exception(
                f"Invalid 'network_data' value: database does not contain table '{NODES_TABLE_NAME}'."
            )

        edges_columns = network_data.edges.column_names
        if SOURCE_COLUMN_NAME not in edges_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{SOURCE_COLUMN_NAME}' column. Available columns: {', '.join(edges_columns)}."
            )
        if TARGET_COLUMN_NAME not in edges_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'edges' table does not contain a '{TARGET_COLUMN_NAME}' column. Available columns: {', '.join(edges_columns)}."
            )

        nodes_columns = network_data.nodes.column_names
        if NODE_ID_COLUMN_NAME not in nodes_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'nodes' table does not contain a '{NODE_ID_COLUMN_NAME}' column. Available columns: {', '.join(nodes_columns)}."
            )
        if LABEL_COLUMN_NAME not in nodes_columns:
            raise Exception(
                f"Invalid 'network_data' value: 'nodes' table does not contain a '{LABEL_COLUMN_NAME}' column. Available columns: {', '.join(nodes_columns)}."
            )

    def pretty_print_as__terminal_renderable(
        self, value: Value, render_config: Mapping[str, Any]
    ) -> Any:

        max_rows = render_config.get(
            "max_no_rows", DEFAULT_PRETTY_PRINT_CONFIG["max_no_rows"]
        )
        max_row_height = render_config.get(
            "max_row_height", DEFAULT_PRETTY_PRINT_CONFIG["max_row_height"]
        )
        max_cell_length = render_config.get(
            "max_cell_length", DEFAULT_PRETTY_PRINT_CONFIG["max_cell_length"]
        )

        half_lines: Union[int, None] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        network_data: NetworkData = value.data

        result: List[Any] = [""]

        nodes_atw = ArrowTabularWrap(network_data.nodes.arrow_table)
        nodes_pretty = nodes_atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]{NODES_TABLE_NAME}[/b]")
        result.append(nodes_pretty)

        edges_atw = ArrowTabularWrap(network_data.edges.arrow_table)
        edges_pretty = edges_atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        result.append(f"[b]{EDGES_TABLE_NAME}[/b]")
        result.append(edges_pretty)

        return Group(*result)

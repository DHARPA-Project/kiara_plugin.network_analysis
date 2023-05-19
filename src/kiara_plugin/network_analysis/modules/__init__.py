# -*- coding: utf-8 -*-
import csv
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Mapping, Union

from pydantic import Field

from kiara.api import KiaraModule, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import (
    FileModel,
)
from kiara.models.module import KiaraModuleConfig
from kiara.models.rendering import RenderValueResult
from kiara.models.values.value import Value
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara_plugin.network_analysis.defaults import (
    DEFAULT_NETWORK_DATA_CHUNK_SIZE,
    ID_COLUMN_NAME,
    LABEL_ALIAS_NAMES,
    LABEL_COLUMN_NAME,
    NODE_ID_ALIAS_NAMES,
    SOURCE_COLUMN_ALIAS_NAMES,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_ALIAS_NAMES,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.utils import (
    insert_table_data_into_network_graph,
    augment_edges_table,
    augment_nodes_table,
)
from kiara_plugin.tabular.models.table import KiaraTable
from kiara_plugin.tabular.modules.db import RenderDatabaseModuleBase
from kiara_plugin.tabular.utils import create_sqlite_schema_data_from_arrow_table

KIARA_METADATA = {
    "authors": [
        {"name": "Lena Jaskov", "email": "helena.jaskov@uni.lu"},
    ],
    "description": "Modules to create/export network data.",
}


class CreateNetworkDataModuleConfig(CreateFromModuleConfig):
    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )


class CreateNetworkDataModule(CreateFromModule):
    _module_type_name = "create.network_data"
    _config_cls = CreateNetworkDataModuleConfig

    def create__network_data__from__file(self, source_value: Value) -> Any:
        """Create a table from a file, trying to auto-determine the format of said file.

        Supported file formats (at the moment):
        - gml
        - gexf
        - graphml (uses the standard xml library present in Python, which is insecure - see xml for additional information. Only parse GraphML files you trust)
        - pajek
        - leda
        - graph6
        - sparse6
        """

        source_file: FileModel = source_value.data
        # the name of the attribute kiara should use to populate the node labels
        label_attr_name: Union[str, None] = None
        # attributes to ignore when creating the node table,
        # mostly useful if we know that the file contains attributes that are not relevant for the network
        # or for 'label', if we don't want to duplicate the information in '_label' and 'label'
        ignore_node_attributes = None

        if source_file.file_name.endswith(".gml"):
            import networkx as nx

            # we use 'lable="id"' here because networkx is fussy about labels being unique and non-null
            # we use the 'label' attribute for the node labels manually later
            graph = nx.read_gml(source_file.path, label="id")
            label_attr_name = "label"
            ignore_node_attributes = ["label"]

        elif source_file.file_name.endswith(".gexf"):
            import networkx as nx

            graph = nx.read_gexf(source_file.path)
        elif source_file.file_name.endswith(".graphml"):
            import networkx as nx

            graph = nx.read_graphml(source_file.path)
        elif source_file.file_name.endswith(".pajek") or source_file.file_name.endswith(
            ".net"
        ):
            import networkx as nx

            graph = nx.read_pajek(source_file.path)
        elif source_file.file_name.endswith(".leda"):
            import networkx as nx

            graph = nx.read_leda(source_file.path)
        elif source_file.file_name.endswith(
            ".graph6"
        ) or source_file.file_name.endswith(".g6"):
            import networkx as nx

            graph = nx.read_graph6(source_file.path)
        elif source_file.file_name.endswith(
            ".sparse6"
        ) or source_file.file_name.endswith(".s6"):
            import networkx as nx

            graph = nx.read_sparse6(source_file.path)
        else:
            raise KiaraProcessingException(
                f"Can't create network data for unsupported format of file: {source_file.file_name}."
            )

        return NetworkData.create_from_networkx_graph(
            graph=graph,
            label_attr_name=label_attr_name,
            ignore_node_attributes=ignore_node_attributes,
        )


class AssembleNetworkDataModuleConfig(KiaraModuleConfig):
    node_id_column_aliases: List[str] = Field(
        description="Alias strings to test (in order) for auto-detecting the node id column.",
        default=NODE_ID_ALIAS_NAMES,
    )  # pydantic should handle that correctly (deepcopy) -- and anyway, it's immutable (hopefully)
    label_column_aliases: List[str] = Field(
        description="Alias strings to test (in order) for auto-detecting the node label column.",
        default=LABEL_ALIAS_NAMES,
    )
    source_column_aliases: List[str] = Field(
        description="Alias strings to test (in order) for auto-detecting the source column.",
        default=SOURCE_COLUMN_ALIAS_NAMES,
    )
    target_column_aliases: List[str] = Field(
        description="Alias strings to test (in order) for auto-detecting the target column.",
        default=TARGET_COLUMN_ALIAS_NAMES,
    )


class AssembleGraphFromTablesModule(KiaraModule):
    """Create a network_data instance from one or two tables.

    This module needs at least one table as input, providing the edges of the resulting network data set.
    If no further table is created, basic node information will be automatically created by using unique values from
    the edges source and target columns.

    If no `source_column_name` (and/or `target_column_name`) is provided, *kiara* will try to auto-detect the most likely
    of the existing columns to use. If that is not possible, an error will be raised.
    """

    _module_type_name = "assemble.network_data.from.tables"
    _config_cls = AssembleNetworkDataModuleConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        inputs: Mapping[str, Any] = {
            "edges": {
                "type": "table",
                "doc": "A table that contains the edges data.",
                "optional": False,
            },
            "source_column": {
                "type": "string",
                "doc": "The name of the source column name in the edges table.",
                "optional": True,
            },
            "target_column": {
                "type": "string",
                "doc": "The name of the target column name in the edges table.",
                "optional": True,
            },
            "edges_column_map": {
                "type": "dict",
                "doc": "An optional map of original column name to desired.",
                "optional": True,
            },
            "nodes": {
                "type": "table",
                "doc": "A table that contains the nodes data.",
                "optional": True,
            },
            "id_column": {
                "type": "string",
                "doc": "The name (before any potential column mapping) of the node-table column that contains the node identifier (used in the edges table).",
                "optional": True,
            },
            "label_column": {
                "type": "string",
                "doc": "The name of a column that contains the node label (before any potential column name mapping). If not specified, the value of the id value will be used as label.",
                "optional": True,
            },
            "nodes_column_map": {
                "type": "dict",
                "doc": "An optional map of original column name to desired.",
                "optional": True,
            },
        }
        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:
        outputs: Mapping[str, Any] = {
            "network_data": {"type": "network_data", "doc": "The network/graph data."}
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        import polars as pl
        import duckdb

        # process nodes
        nodes = inputs.get_value_obj("nodes")

        nodes_column_map: Dict[str, str] = inputs.get_value_data("nodes_column_map")
        if nodes_column_map is None:
            nodes_column_map = {}

        # we need to process the nodes first, because if we have nodes, we need to create the node id map that translates from the original
        # id to the new, internal, integer-based one
        if nodes.is_set:
            for col_name in nodes_column_map.values():
                if col_name.startswith("_"):
                    raise KiaraProcessingException(
                        f"Nodes column map contains target column name that starts with an underscore ('{col_name}'). This is not allowed."
                    )

            nodes_table: KiaraTable = nodes.data
            assert nodes_table is not None

            nodes_column_names = nodes_table.column_names

            # TODO: deal with the case that we are importing a table that was exported from an earlier network_data instance
            for col_name in nodes_column_names:
                if col_name.startswith("_"):
                    raise KiaraProcessingException(
                        f"Nodes table contains column that starts with an underscore ('{col_name}'). This is not allowed."
                    )

            # the most important column is the id column, which is the only one that we absolute need to have
            id_column_name = inputs.get_value_data("id_column")
            if id_column_name is None:
                column_names_to_test = self.get_config_value("node_id_column_aliases")
                for col_name in nodes_column_names:
                    if col_name.lower() in column_names_to_test:
                        id_column_name = col_name
                        break
                if id_column_name is None:
                    raise KiaraProcessingException(
                        f"Could not auto-determine id column name. Please specify one manually, using one of: {', '.join(nodes_column_names)}"
                    )

            if id_column_name not in nodes_column_names:
                raise KiaraProcessingException(
                    f"Could not find id column '{id_column_name}' in the nodes table. Please specify a valid column name manually, using one of: {', '.join(nodes_column_names)}"
                )

            # the label is optional, if not specified, we try to auto-detect it. If not possible, we will use the (stringified) id column as label.
            label_column_name = inputs.get_value_data("label_column")
            if label_column_name is None:
                column_names_to_test = self.get_config_value("label_column_aliases")
                for col_name in nodes_column_names:
                    if col_name.lower() in column_names_to_test:
                        label_column_name = col_name
                        break

            if label_column_name and label_column_name not in nodes_column_names:
                raise KiaraProcessingException(
                    f"Could not find id column '{id_column_name}' in the nodes table. Please specify a valid column name manually, using one of: {', '.join(nodes_column_names)}"
                )

            nodes_arrow_dataframe = nodes_table.polars_dataframe
            nullable_columns = [
                col_name
                for col_name in nodes_arrow_dataframe.columns
                if col_name not in [ID_COLUMN_NAME, LABEL_COLUMN_NAME]
            ]

        else:
            nodes_arrow_dataframe = None
            nullable_columns = []
            label_column_name = None

        # process edges

        edges = inputs.get_value_obj("edges")
        edges_table: KiaraTable = edges.data
        edges_source_column_name = inputs.get_value_data("source_column")
        edges_target_column_name = inputs.get_value_data("target_column")

        edges_arrow_dataframe = edges_table.polars_dataframe
        edges_column_names = edges_arrow_dataframe.columns

        if edges_source_column_name is None:
            column_names_to_test = self.get_config_value("source_column_aliases")
            for item in edges_column_names:
                if item.lower() in column_names_to_test:
                    edges_source_column_name = item
                    break

        if edges_target_column_name is None:
            column_names_to_test = self.get_config_value("target_column_aliases")
            for item in edges_column_names:
                if item.lower() in column_names_to_test:
                    edges_target_column_name = item
                    break

        if not edges_source_column_name or not edges_target_column_name:
            if not edges_source_column_name and not edges_target_column_name:
                if len(edges_column_names) == 2:
                    edges_source_column_name = edges_column_names[0]
                    edges_target_column_name = edges_column_names[1]
                else:
                    raise KiaraProcessingException(
                        f"Could not auto-detect source and target column names. Please specify them manually using one of: {', '.join(edges_column_names)}."
                    )

            if not edges_source_column_name:
                raise KiaraProcessingException(
                    f"Could not auto-detect source column name. Please specify it manually using one of: {', '.join(edges_column_names)}."
                )

            if not edges_target_column_name:
                raise KiaraProcessingException(
                    f"Could not auto-detect target column name. Please specify it manually using one of: {', '.join(edges_column_names)}."
                )

        edges_column_map: Dict[str, str] = inputs.get_value_data("edges_column_map")
        if edges_column_map is None:
            edges_column_map = {}
        if edges_source_column_name in edges_column_map.keys():
            raise KiaraProcessingException(
                "The value of the 'source_column_name' argument is not allowed in the edges column map."
            )
        if edges_target_column_name in edges_column_map.keys():
            raise KiaraProcessingException(
                "The value of the 'source_column_name' argument is not allowed in the edges column map."
            )

        for v in edges_column_map.values():
            if v.startswith("_"):
                raise KiaraProcessingException(
                    "Mapped column names in the edges column map must not start with an underscore ('_')."
                )

        if edges_source_column_name not in edges_column_names:
            raise KiaraProcessingException(
                f"Edges table does not contain source column '{edges_source_column_name}'. Choose one of: {', '.join(edges_column_names)}."
            )
        if edges_target_column_name not in edges_column_names:
            raise KiaraProcessingException(
                f"Edges table does not contain target column '{edges_target_column_name}'. Choose one of: {', '.join(edges_column_names)}."
            )

        for column_name in edges_column_map.keys():
            if (
                column_name.startswith("_")
                and column_name not in edges_column_map.keys()
            ):
                raise KiaraProcessingException(
                    f"Column name '{column_name}' starts with an underscore, that is not allowed."
                )

        source_column_old = edges_arrow_dataframe.get_column(edges_source_column_name)
        target_column_old = edges_arrow_dataframe.get_column(edges_target_column_name)

        # fill out the node id map
        unique_node_ids_old = pl.concat(
            [source_column_old, target_column_old], rechunk=False
        ).unique()

        if nodes_arrow_dataframe is None:
            new_node_ids = range(0, len(unique_node_ids_old))
            node_id_map = {
                node_id: new_node_id
                for node_id, new_node_id in zip(unique_node_ids_old, new_node_ids)
            }

            nodes_arrow_dataframe = pl.DataFrame(
                {
                    ID_COLUMN_NAME: new_node_ids,
                    LABEL_COLUMN_NAME: (str(x) for x in unique_node_ids_old),
                    "id": unique_node_ids_old,
                }
            )
            unique_node_columns = [ID_COLUMN_NAME, "id"]

        else:
            id_column_old = nodes_arrow_dataframe.get_column(id_column_name)
            old_len = len(unique_node_ids_old)
            if len(unique_node_ids_old) > old_len:
                raise NotImplementedError()
            else:
                new_node_ids = range(0, len(id_column_old))
                node_id_map = {
                    node_id: new_node_id
                    for node_id, new_node_id in zip(id_column_old, new_node_ids)
                }
                new_idx_series = pl.Series(name=ID_COLUMN_NAME, values=new_node_ids)
                nodes_arrow_dataframe.insert_at_idx(0, new_idx_series)

                if label_column_name is None:
                    label_column_name = ID_COLUMN_NAME

                # we create a copy of the label column, and stringify its items
                label_column = nodes_arrow_dataframe.get_column(
                    label_column_name
                ).rename(LABEL_COLUMN_NAME)
                if label_column.dtype != pl.Utf8:
                    label_column = label_column.cast(pl.Utf8)

                if label_column.null_count() != 0:
                    raise KiaraProcessingException(
                        f"Label column '{label_column_name}' contains null values. This is not allowed."
                    )

                nodes_arrow_dataframe = nodes_arrow_dataframe.insert_at_idx(
                    1, label_column
                )

            unique_node_columns = [ID_COLUMN_NAME, id_column_name]

        source_column_mapped = source_column_old.map_dict(
            node_id_map, default=None
        ).rename(SOURCE_COLUMN_NAME)
        if source_column_mapped.is_null().any():
            raise KiaraProcessingException(
                "The source column contains values that are not mapped in the nodes table."
            )
        target_column_mapped = target_column_old.map_dict(
            node_id_map, default=None
        ).rename(TARGET_COLUMN_NAME)
        if target_column_mapped.is_null().any():
            raise KiaraProcessingException(
                "The target column contains values that are not mapped in the nodes table."
            )

        edges_arrow_dataframe.insert_at_idx(0, source_column_mapped)
        edges_arrow_dataframe.insert_at_idx(1, target_column_mapped)

        edges_arrow_dataframe = edges_arrow_dataframe.drop(edges_source_column_name)
        edges_arrow_dataframe = edges_arrow_dataframe.drop(edges_target_column_name)

        # edges_arrow_table = edges_arrow_dataframe.to_arrow()

        edges_table_augmented = augment_edges_table(edges_arrow_dataframe)

        # TODO: also index the other columns?
        edges_data_schema = create_sqlite_schema_data_from_arrow_table(
            table=edges_table_augmented,
            index_columns=[SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME],
            column_map=edges_column_map,
        )

        nodes_arrow_table = nodes_arrow_dataframe.to_arrow()

        nodes_table_augmented = augment_nodes_table(
            nodes_arrow_table, edges_table_augmented
        )

        nodes_data_schema = create_sqlite_schema_data_from_arrow_table(
            table=nodes_arrow_table,
            index_columns=[ID_COLUMN_NAME, LABEL_COLUMN_NAME],
            column_map=nodes_column_map,
            nullable_columns=nullable_columns,
            unique_columns=unique_node_columns,
        )

        network_data = NetworkData.create_network_data_in_temp_dir(
            schema_edges=edges_data_schema,
            schema_nodes=nodes_data_schema,
            keep_unlocked=True,
        )

        insert_table_data_into_network_graph(
            network_data=network_data,
            edges_table=edges_table_augmented,
            edges_column_map=edges_column_map,
            nodes_table=nodes_arrow_table,
            nodes_column_map=nodes_column_map,
            chunk_size=DEFAULT_NETWORK_DATA_CHUNK_SIZE,
        )

        network_data._lock_db()

        outputs.set_value("network_data", network_data)


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
        graph: nx.Graph = value.as_networkx_graph(nx.DiGraph)
        nx.write_graphml(graph, target_path)

        return {"files": target_path}

    def export__network_data__as__sqlite_db(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as a sqlite database file."""

        target_path = os.path.abspath(os.path.join(base_path, f"{name}.sqlite"))
        shutil.copy2(value.db_file_path, target_path)

        return {"files": target_path}

    def export__network_data__as__sql_dump(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as a sql dump file."""

        import sqlite_utils

        db = sqlite_utils.Database(value.db_file_path)
        target_path = Path(os.path.join(base_path, f"{name}.sql"))
        with target_path.open("wt") as f:
            for line in db.conn.iterdump():
                f.write(line + "\n")

        return {"files": target_path.as_posix()}

    def export__network_data__as__csv_files(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as 2 csv files (one for edges, one for nodes."""

        import sqlite3

        files = []

        for table_name in value.table_names:
            target_path = os.path.join(base_path, f"{name}__{table_name}.csv")
            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            # copied from: https://stackoverflow.com/questions/2952366/dump-csv-from-sqlalchemy
            con = sqlite3.connect(value.db_file_path)
            outfile = open(target_path, "wt")
            outcsv = csv.writer(outfile)

            cursor = con.execute(f"select * from {table_name}")
            # dump column titles (optional)
            outcsv.writerow(x[0] for x in cursor.description)
            # dump rows
            outcsv.writerows(cursor.fetchall())

            outfile.close()
            files.append(target_path)

        return {"files": files}


class RenderNetworkModule(RenderDatabaseModuleBase):
    _module_type_name = "render.network_data.for.web"

    def render__network_data__as__html(
        self, value: Value, render_config: Mapping[str, Any]
    ):
        input_number_of_rows = render_config.get("number_of_rows", 20)
        input_row_offset = render_config.get("row_offset", 0)

        table_name = render_config.get("table_name", None)

        wrap, data_related_scenes = self.preprocess_database(
            value=value,
            table_name=table_name,
            input_number_of_rows=input_number_of_rows,
            input_row_offset=input_row_offset,
        )
        pretty = wrap.as_html(max_row_height=1)

        result = RenderValueResult(
            value_id=value.value_id,
            render_config=render_config,
            render_manifest=self.manifest.manifest_hash,
            rendered=pretty,
            related_scenes=data_related_scenes,
        )
        return result

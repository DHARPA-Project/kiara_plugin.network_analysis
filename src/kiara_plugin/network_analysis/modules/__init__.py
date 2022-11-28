# -*- coding: utf-8 -*-
import csv
import os
import shutil
from pathlib import Path
from typing import Any, Dict, Mapping, Union

from kiara import KiaraModule, Value, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara.models.rendering import RenderValueResult
from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara_plugin.tabular.models.table import KiaraTable
from kiara_plugin.tabular.modules.db import RenderDatabaseModuleBase
from kiara_plugin.tabular.utils import create_sqlite_schema_data_from_arrow_table

from kiara_plugin.network_analysis.defaults import (
    DEFAULT_NETWORK_DATA_CHUNK_SIZE,
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.utils import insert_table_data_into_network_graph


class CreateGraphFromTablesModule(KiaraModule):
    """Create a graph object from one or two tables.

    This module needs at least one table as input, providing the edges of the resulting network data set.
    If no further table is created, basic node information will be automatically created by using unique values from
    the edges source and target columns.
    """

    _module_type_name = "create.network_data.from.tables"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        inputs: Mapping[str, Any] = {
            "edges": {
                "type": "table",
                "doc": "A table that contains the edges data.",
                "optional": False,
            },
            "source_column_name": {
                "type": "string",
                "doc": "The name of the source column name in the edges table.",
                "default": SOURCE_COLUMN_NAME,
            },
            "target_column_name": {
                "type": "string",
                "doc": "The name of the target column name in the edges table.",
                "default": TARGET_COLUMN_NAME,
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
            "id_column_name": {
                "type": "string",
                "doc": "The name (before any potential column mapping) of the node-table column that contains the node identifier (used in the edges table).",
                "default": ID_COLUMN_NAME,
            },
            "label_column_name": {
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

        edges = inputs.get_value_obj("edges")
        edges_table: KiaraTable = edges.data
        edges_source_column_name = inputs.get_value_data("source_column_name")
        edges_target_column_name = inputs.get_value_data("target_column_name")

        edges_columns = edges_table.column_names
        if edges_source_column_name not in edges_columns:
            raise KiaraProcessingException(
                f"Edges table does not contain source column '{edges_source_column_name}'. Choose one of: {', '.join(edges_columns)}."
            )
        if edges_target_column_name not in edges_columns:
            raise KiaraProcessingException(
                f"Edges table does not contain target column '{edges_source_column_name}'. Choose one of: {', '.join(edges_columns)}."
            )

        nodes = inputs.get_value_obj("nodes")

        id_column_name = inputs.get_value_data("id_column_name")
        label_column_name = inputs.get_value_data("label_column_name")
        nodes_column_map: Dict[str, str] = inputs.get_value_data("nodes_column_map")
        if nodes_column_map is None:
            nodes_column_map = {}

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

        edges_column_map[edges_source_column_name] = SOURCE_COLUMN_NAME
        edges_column_map[edges_target_column_name] = TARGET_COLUMN_NAME

        edges_data_schema = create_sqlite_schema_data_from_arrow_table(
            table=edges_table.arrow_table,
            index_columns=[SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME],
            column_map=edges_column_map,
        )

        nodes_table: Union[KiaraTable, None] = None
        if nodes.is_set:
            if (
                id_column_name in nodes_column_map.keys()
                and nodes_column_map[id_column_name] != ID_COLUMN_NAME
            ):
                raise KiaraProcessingException(
                    "The value of the 'id_column_name' argument is not allowed in the node column map."
                )

            nodes_column_map[id_column_name] = ID_COLUMN_NAME

            nodes_table = nodes.data

            assert nodes_table is not None

            extra_schema = []
            if label_column_name is None:
                label_column_name = LABEL_COLUMN_NAME

            for cn in nodes_table.column_names:
                if cn.lower() == LABEL_COLUMN_NAME.lower():
                    label_column_name = cn
                    break

            if LABEL_COLUMN_NAME in nodes_table.column_names:
                if label_column_name != LABEL_COLUMN_NAME:
                    raise KiaraProcessingException(
                        f"Can't create database for graph data: original data contains column called 'label', which is a protected column name. If this column can be used as a label, remove your '{label_column_name}' input value for the 'label_column_name' input and re-run this module."
                    )

            if label_column_name in nodes_table.column_names:
                if label_column_name in nodes_column_map.keys():
                    raise KiaraProcessingException(
                        "The value of the 'label_column_name' argument is not allowed in the node column map."
                    )
            else:
                extra_schema.append("    label    TEXT")

            nodes_column_map[label_column_name] = LABEL_COLUMN_NAME

            nullable_columns = list(nodes_table.column_names)
            if ID_COLUMN_NAME in nullable_columns:
                nullable_columns.remove(ID_COLUMN_NAME)

            nodes_data_schema = create_sqlite_schema_data_from_arrow_table(
                table=nodes_table.arrow_table,
                index_columns=[ID_COLUMN_NAME],
                column_map=nodes_column_map,
                nullable_columns=[],
                unique_columns=[ID_COLUMN_NAME],
            )

        else:
            nodes_data_schema = None

        network_data = NetworkData.create_network_data_in_temp_dir(
            schema_edges=edges_data_schema,
            schema_nodes=nodes_data_schema,
            keep_unlocked=True,
        )

        insert_table_data_into_network_graph(
            network_data=network_data,
            edges_table=edges_table.arrow_table,
            edges_column_map=edges_column_map,
            nodes_table=None if nodes_table is None else nodes_table.arrow_table,
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

        return {"files": target_path}

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

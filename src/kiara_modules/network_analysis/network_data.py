# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import typing

from kiara import KiaraModule
from kiara.data import ValueSet
from kiara.data.values import ValueSchema
from kiara.exceptions import KiaraProcessingException
from kiara_modules.core.database import (
    BaseDatabaseInfoMetadataModule,
    BaseDatabaseMetadataModule,
    BaseStoreDatabaseTypeModule,
)
from kiara_modules.core.table.utils import create_sqlite_schema_from_arrow_table

from kiara_modules.network_analysis.defaults import (
    ID_COLUMN_NAME,
    LABEL_COLUMN_NAME,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_NAME,
)
from kiara_modules.network_analysis.metadata_schemas import NetworkData

if typing.TYPE_CHECKING:
    import pyarrow as pa


class StoreNetworkDataTypeModule(BaseStoreDatabaseTypeModule):
    """Save network data a sqlite database file."""

    _module_type_name = "store"

    @classmethod
    def retrieve_supported_types(cls) -> typing.Union[str, typing.Iterable[str]]:
        return "network_data"


class DatabaseMetadataModule(BaseDatabaseMetadataModule):
    """Extract basic metadata from a database object."""

    _module_type_name = "metadata"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "network_data"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "database"


class DatabaseInfoMetadataModule(BaseDatabaseInfoMetadataModule):
    """Extract extended metadata (like tables, schemas) from a database object."""

    _module_type_name = "info"

    @classmethod
    def _get_supported_types(cls) -> str:
        return "network_data"

    @classmethod
    def get_metadata_key(cls) -> str:
        return "database_info"


class CreateGraphFromFilesModule(KiaraModule):
    """Create a graph object from a file."""

    _module_type_name = "from_tables"

    def create_input_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        inputs: typing.Mapping[str, typing.Any] = {
            "edges": {
                "type": "table",
                "doc": "A table that contains the edges data.",
                "optional": False,
            },
            "edges_source_column_name": {
                "type": "string",
                "doc": "The name of the source column name in the edges table.",
                "default": "source",
            },
            "edges_target_column_name": {
                "type": "string",
                "doc": "The name of the target column name in the edges table.",
                "default": "target",
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
                "default": "id",
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

    def create_output_schema(
        self,
    ) -> typing.Mapping[
        str, typing.Union[ValueSchema, typing.Mapping[str, typing.Any]]
    ]:

        outputs: typing.Mapping[str, typing.Any] = {
            "network_data": {"type": "network_data", "doc": "The network/graph data."}
        }
        return outputs

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:

        edges = inputs.get_value_obj("edges")
        edges_source_column_name = inputs.get_value_data("edges_source_column_name")
        edges_target_column_name = inputs.get_value_data("edges_target_column_name")

        edges_columns = edges.get_metadata("table")["table"]["column_names"]
        if edges_source_column_name not in edges_columns:
            raise KiaraProcessingException(
                f"Edges table does not contain source column '{edges_source_column_name}'. Choose one of: {', '.join(edges_columns)}."
            )
        if edges_target_column_name not in edges_columns:
            raise KiaraProcessingException(
                f"Edges table does not contain target column '{edges_source_column_name}'. Choose one of: {', '.join(edges_columns)}."
            )

        nodes = inputs.get_value_obj("nodes")

        DEFAULT_DB_CHUNK_SIZE = 1024
        network_data = NetworkData.create_in_temp_dir()

        added_node_ids = set()
        edges_table: pa.Table = edges.get_value_data()

        id_column_name = inputs.get_value_data("id_column_name")
        label_column_name = inputs.get_value_data("label_column_name")
        nodes_column_map: typing.Dict[str, str] = inputs.get_value_data(
            "nodes_column_map"
        )
        if nodes_column_map is None:
            nodes_column_map = {}

        nodes_table: typing.Optional[pa.Table] = None
        if nodes.is_set:

            if id_column_name in nodes_column_map.keys():
                raise KiaraProcessingException(
                    "The value of the 'id_column_name' argument is not allowed in the node column map."
                )

            nodes_column_map[id_column_name] = ID_COLUMN_NAME

            nodes_table = nodes.get_value_data()

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
            schema_sql = (
                create_sqlite_schema_from_arrow_table(
                    table=nodes_table,
                    table_name="nodes",
                    index_columns=["id"],
                    column_map=nodes_column_map,
                    extra_column_info={"id": "NOT NULL UNIQUE"},
                    extra_schema=extra_schema,
                )
                + "\n"
            )

        else:
            schema_sql = """CREATE TABLE IF NOT EXISTS nodes (
    id    INTEGER    NOT NULL UNIQUE,
    label    TEXT
);
CREATE INDEX IF NOT EXISTS id_idx ON nodes(id);
"""

        edges_column_map: typing.Dict[str, str] = inputs.get_value_data(
            "edges_column_map"
        )
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

        edges_sql = create_sqlite_schema_from_arrow_table(
            table=edges_table,
            table_name="edges",
            index_columns=["source", "target"],
            column_map=edges_column_map,
            extra_schema=[
                "    FOREIGN KEY(source) REFERENCES nodes(id)",
                "    FOREIGN KEY(target) REFERENCES nodes(id)",
            ],
        )

        schema_sql = f"{schema_sql}{edges_sql}"

        network_data = NetworkData.create_in_temp_dir()

        # create table schema
        network_data.execute_sql(schema_sql)

        # =============================================
        # import data

        if nodes_table is not None:
            for batch in nodes_table.to_batches(DEFAULT_DB_CHUNK_SIZE):
                batch_dict = batch.to_pydict()

                for k, v in nodes_column_map.items():
                    if k in batch_dict.keys():
                        if k == ID_COLUMN_NAME and v == LABEL_COLUMN_NAME:
                            _data = batch_dict.get(k)
                        else:
                            _data = batch_dict.pop(k)
                            if v in batch_dict.keys():
                                raise Exception(
                                    "Duplicate nodes column name after mapping: {v}"
                                )
                        batch_dict[v] = _data

                ids = batch_dict[ID_COLUMN_NAME]
                data = [dict(zip(batch_dict, t)) for t in zip(*batch_dict.values())]
                network_data.insert_nodes(*data)

                added_node_ids.update(ids)

        for batch in edges_table.to_batches(DEFAULT_DB_CHUNK_SIZE):

            batch_dict = batch.to_pydict()
            for k, v in edges_column_map.items():
                if k in batch_dict.keys():
                    _data = batch_dict.pop(k)
                    if v in batch_dict.keys():
                        raise Exception(
                            "Duplicate edges column name after mapping: {v}"
                        )
                    batch_dict[v] = _data

            data = [dict(zip(batch_dict, t)) for t in zip(*batch_dict.values())]

            all_node_ids = network_data.insert_edges(
                *data,
                existing_node_ids=added_node_ids,
            )
            added_node_ids.update(all_node_ids)

        outputs.set_value("network_data", network_data)

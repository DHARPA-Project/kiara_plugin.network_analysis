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

from kiara_modules.network_analysis.metadata_schemas import NetworkData


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
            "nodes": {
                "type": "table",
                "doc": "A table that contains the nodes data.",
                "optional": True,
            },
            "id_column_name": {
                "type": "string",
                "doc": "The name of the column that contains the node identifier (which is used in the sources).",
                "default": "id",
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
        id_column_name = inputs.get_value_data("id_column_name")

        import pyarrow as pa

        CHUNK_SIZE = 1024
        network_data = NetworkData.create_in_temp_dir()

        added_node_ids = set()

        if nodes is not None:
            nodes_columns = nodes.get_metadata("table")["table"]["column_names"]
            if id_column_name not in nodes_columns:
                raise KiaraProcessingException(
                    f"Nodes table does not contain id column '{id_column_name}'. Choose one of: {', '.join(nodes_columns)}."
                )

            nodes_table: pa.Table = nodes.get_value_data()
            for batch in nodes_table.to_batches(CHUNK_SIZE):
                batch_dict = batch.to_pydict()
                ids = batch_dict.pop(id_column_name)

                keys = ["id", *batch_dict.keys()]
                data = (row for row in zip(ids, *batch_dict.values()))

                def assemble_dict(d):
                    result = {"id": d[0]}
                    for i in range(0, len(keys)):
                        result[keys[i]] = d[i]
                    return result

                dicts = map(assemble_dict, data)
                network_data.upsert_nodes(ids, dicts)
                added_node_ids.update(ids)

        edges_table: pa.Table = edges.get_value_data()
        for batch in edges_table.to_batches(CHUNK_SIZE):
            all_node_ids = network_data.insert_edges(
                batch.to_pydict(),
                source_column_name=edges_source_column_name,
                target_column_name=edges_target_column_name,
                existing_node_ids=added_node_ids,
            )
            added_node_ids.update(all_node_ids)

        outputs.set_value("network_data", network_data)

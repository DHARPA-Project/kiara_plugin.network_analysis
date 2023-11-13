# -*- coding: utf-8 -*-
from typing import Any, Dict, List, Mapping, Union

from pydantic import Field

from kiara.api import KiaraModule, ValueMap, ValueMapSchema
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import (
    KiaraFile,
)
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import JobLog
from kiara.models.values.value import Value
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara_plugin.network_analysis.defaults import (
    LABEL_ALIAS_NAMES,
    LABEL_COLUMN_NAME,
    NODE_ID_ALIAS_NAMES,
    NODE_ID_COLUMN_NAME,
    SOURCE_COLUMN_ALIAS_NAMES,
    SOURCE_COLUMN_NAME,
    TARGET_COLUMN_ALIAS_NAMES,
    TARGET_COLUMN_NAME,
)
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.tabular.models.table import KiaraTable

KIARA_METADATA = {
    "authors": [
        {"name": "Lena Jaskov", "email": "helena.jaskov@uni.lu"},
        {"name": "Markus Binsteiner", "email": "markus@frkl.io"},
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

        source_file: KiaraFile = source_value.data
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
            supported_file_estensions = [
                "gml",
                "gexf",
                "graphml",
                "pajek",
                "leda",
                "graph6",
                "g6",
                "sparse6",
                "s6",
            ]

            msg = f"Can't create network data for unsupported format of file: {source_file.file_name}. Supported file extensions: {', '.join(supported_file_estensions)}"

            raise KiaraProcessingException(msg)

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
    """Create a 'network_data' instance from one or two tables.

    This module needs at least one table as input, providing the edges of the resulting network data set.
    If no further table is created, basic node information will be automatically created by using unique values from the edges source and target columns.

    If no `source_column_name` (and/or `target_column_name`) is provided, *kiara* will try to auto-detect the most likely of the existing columns to use. If that is not possible, an error will be raised.
    """

    _module_type_name = "assemble.network_data"
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

    def process(self, inputs: ValueMap, outputs: ValueMap, job_log: JobLog) -> None:

        import polars as pl

        # process nodes
        nodes = inputs.get_value_obj("nodes")

        # the nodes column map can be used to rename attribute columns in the nodes table
        nodes_column_map: Dict[str, str] = inputs.get_value_data("nodes_column_map")
        if nodes_column_map is None:
            nodes_column_map = {}

        # we need to process the nodes first, because if we have nodes, we need to create the node id map that translates from the original
        # id to the new, internal, integer-based one

        if nodes.is_set:

            job_log.add_log("processing nodes table")

            nodes_table: KiaraTable = nodes.data
            assert nodes_table is not None

            nodes_column_names = nodes_table.column_names

            # the most important column is the id column, which is the only one that we absolutely need to have
            id_column_name = inputs.get_value_data("id_column")

            if id_column_name is None:
                # try to auto-detect the id column
                column_names_to_test = self.get_config_value("node_id_column_aliases")
                for col_name in nodes_column_names:
                    if col_name.lower() in column_names_to_test:
                        id_column_name = col_name
                        break

                job_log.add_log(f"auto-detected id column: {id_column_name}")
                if id_column_name is None:
                    raise KiaraProcessingException(
                        f"Could not auto-determine id column name. Please specify one manually, using one of: {', '.join(nodes_column_names)}"
                    )

            if id_column_name not in nodes_column_names:
                raise KiaraProcessingException(
                    f"Could not find id column '{id_column_name}' in the nodes table. Please specify a valid column name manually, using one of: {', '.join(nodes_column_names)}"
                )

            nodes_column_map[id_column_name] = NODE_ID_COLUMN_NAME
            if id_column_name in nodes_column_map.keys():
                if nodes_column_map[id_column_name] != NODE_ID_COLUMN_NAME:
                    raise KiaraProcessingException(
                        f"Existing mapping of id column name '{id_column_name}' is not mapped to '{NODE_ID_COLUMN_NAME}' in the 'nodes_column_map' input."
                    )
            else:
                nodes_column_map[id_column_name] = NODE_ID_COLUMN_NAME

            # the label is optional, if not specified, we try to auto-detect it. If not possible, we will use the (stringified) id column as label.
            label_column_name = inputs.get_value_data("label_column")
            if label_column_name is None:
                job_log.add_log("auto-detecting label column")
                column_names_to_test = self.get_config_value("label_column_aliases")
                for col_name in nodes_column_names:
                    if col_name.lower() in column_names_to_test:
                        label_column_name = col_name
                        job_log.add_log(
                            f"auto-detected label column: {label_column_name}"
                        )
                        break

            if label_column_name and label_column_name not in nodes_column_names:
                raise KiaraProcessingException(
                    f"Could not find id column '{id_column_name}' in the nodes table. Please specify a valid column name manually, using one of: {', '.join(nodes_column_names)}"
                )

            nodes_arrow_dataframe = nodes_table.to_polars_dataframe()

        else:
            nodes_arrow_dataframe = None
            label_column_name = None

        # process edges

        job_log.add_log("processing edges table")
        edges = inputs.get_value_obj("edges")
        edges_table: KiaraTable = edges.data
        edges_source_column_name = inputs.get_value_data("source_column")
        edges_target_column_name = inputs.get_value_data("target_column")

        edges_arrow_dataframe = edges_table.to_polars_dataframe()
        edges_column_names = edges_arrow_dataframe.columns

        if edges_source_column_name is None:
            job_log.add_log("auto-detecting source column")
            column_names_to_test = self.get_config_value("source_column_aliases")
            for item in edges_column_names:
                if item.lower() in column_names_to_test:
                    edges_source_column_name = item
                    job_log.add_log(
                        f"auto-detected source column: {edges_source_column_name}"
                    )
                    break

        if edges_target_column_name is None:
            job_log.add_log("auto-detecting target column")
            column_names_to_test = self.get_config_value("target_column_aliases")
            for item in edges_column_names:
                if item.lower() in column_names_to_test:
                    edges_target_column_name = item
                    job_log.add_log(
                        f"auto-detected target column: {edges_target_column_name}"
                    )
                    break

        if not edges_source_column_name or not edges_target_column_name:
            if not edges_source_column_name and not edges_target_column_name:
                if len(edges_column_names) == 2:
                    job_log.add_log(
                        "using first two columns as source and target columns"
                    )
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
            if edges_column_map[edges_source_column_name] != SOURCE_COLUMN_NAME:
                raise KiaraProcessingException(
                    f"Existing mapping of source column name '{edges_source_column_name}' is not mapped to '{SOURCE_COLUMN_NAME}' in the 'edges_column_map' input."
                )
        else:
            edges_column_map[edges_source_column_name] = SOURCE_COLUMN_NAME

        if edges_target_column_name in edges_column_map.keys():
            if edges_column_map[edges_target_column_name] == SOURCE_COLUMN_NAME:
                raise KiaraProcessingException(
                    msg="Edges and source column names can't be the same."
                )
            if edges_column_map[edges_target_column_name] != TARGET_COLUMN_NAME:
                raise KiaraProcessingException(
                    f"Existing mapping of target column name '{edges_target_column_name}' is not mapped to '{TARGET_COLUMN_NAME}' in the 'edges_column_map' input."
                )
        else:
            edges_column_map[edges_target_column_name] = TARGET_COLUMN_NAME

        if edges_source_column_name not in edges_column_names:
            raise KiaraProcessingException(
                f"Edges table does not contain source column '{edges_source_column_name}'. Choose one of: {', '.join(edges_column_names)}."
            )
        if edges_target_column_name not in edges_column_names:
            raise KiaraProcessingException(
                f"Edges table does not contain target column '{edges_target_column_name}'. Choose one of: {', '.join(edges_column_names)}."
            )

        source_column_old = edges_arrow_dataframe.get_column(edges_source_column_name)
        target_column_old = edges_arrow_dataframe.get_column(edges_target_column_name)

        job_log.add_log("generating node id map and nodes table")
        # fill out the node id map
        unique_node_ids_old = (
            pl.concat([source_column_old, target_column_old], rechunk=False)
            .unique()
            .sort()
        )

        if nodes_arrow_dataframe is None:
            new_node_ids = range(0, len(unique_node_ids_old))  # noqa: PIE808
            node_id_map = dict(zip(unique_node_ids_old, new_node_ids))
            # node_id_map = {
            #     node_id: new_node_id
            #     for node_id, new_node_id in
            # }

            nodes_arrow_dataframe = pl.DataFrame(
                {
                    NODE_ID_COLUMN_NAME: new_node_ids,
                    LABEL_COLUMN_NAME: (str(x) for x in unique_node_ids_old),
                    "id": unique_node_ids_old,
                }
            )

        else:
            id_column_old = nodes_arrow_dataframe.get_column(id_column_name)
            unique_node_ids_nodes_table = id_column_old.unique().sort()

            if len(unique_node_ids_old) > len(unique_node_ids_nodes_table):
                ~(unique_node_ids_old.is_in(unique_node_ids_nodes_table))
                raise NotImplementedError("MISSING NODE IDS NOT IMPLEMENTED YET")
            else:
                new_node_ids = range(0, len(id_column_old))  # noqa: PIE808
                node_id_map = dict(zip(id_column_old, new_node_ids))
                # node_id_map = {
                #     node_id: new_node_id
                #     for node_id, new_node_id in
                # }
                new_idx_series = pl.Series(
                    name=NODE_ID_COLUMN_NAME, values=new_node_ids
                )
                nodes_arrow_dataframe.insert_at_idx(0, new_idx_series)

                if not label_column_name:
                    label_column_name = NODE_ID_COLUMN_NAME

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

        # TODO: deal with different types if node ids are strings or integers
        try:
            source_column_mapped = source_column_old.map_dict(
                node_id_map, default=None
            ).rename(SOURCE_COLUMN_NAME)
        except Exception:
            raise KiaraProcessingException(
                "Could not map node ids onto edges source column.  In most cases the issue is that your node ids have a different data type in your nodes table as in the source column of your edges table."
            )

        if source_column_mapped.is_null().any():
            raise KiaraProcessingException(
                "The source column contains values that are not mapped in the nodes table."
            )

        try:
            target_column_mapped = target_column_old.map_dict(
                node_id_map, default=None
            ).rename(TARGET_COLUMN_NAME)
        except Exception:
            raise KiaraProcessingException(
                "Could not map node ids onto edges source column.  In most cases the issue is that your node ids have a different data type in your nodes table as in the target column of your edges table."
            )

        if target_column_mapped.is_null().any():
            raise KiaraProcessingException(
                "The target column contains values that are not mapped in the nodes table."
            )

        edges_arrow_dataframe.insert_at_idx(0, source_column_mapped)
        edges_arrow_dataframe.insert_at_idx(1, target_column_mapped)

        edges_arrow_dataframe = edges_arrow_dataframe.drop(edges_source_column_name)
        edges_arrow_dataframe = edges_arrow_dataframe.drop(edges_target_column_name)

        edges_arrow_table = edges_arrow_dataframe.to_arrow()
        # edges_table_augmented = augment_edges_table_with_weights(edges_arrow_dataframe)

        # # TODO: also index the other columns?
        # edges_data_schema = create_sqlite_schema_data_from_arrow_table(
        #     table=edges_arrow_dataframe,
        #     index_columns=[SOURCE_COLUMN_NAME, TARGET_COLUMN_NAME],
        #     column_map=edges_column_map,
        # )

        nodes_arrow_table = nodes_arrow_dataframe.to_arrow()

        job_log.add_log("creating network data instance")
        network_data = NetworkData.create_network_data(
            nodes_table=nodes_arrow_table, edges_table=edges_arrow_table
        )

        outputs.set_value("network_data", network_data)


# class FilteredNetworkDataModule(KiaraModule):
#     """Create a new network_data instance from an existing one, using only a sub-set of nodes and/or edges."""
#
#     def create_inputs_schema(
#         self,
#     ) -> ValueMapSchema:
#         return {}
#
#     def create_outputs_schema(
#         self,
#     ) -> ValueMapSchema:
#         return {}

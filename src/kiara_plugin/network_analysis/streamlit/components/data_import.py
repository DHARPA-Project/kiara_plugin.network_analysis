# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Union

from kiara.interfaces.python_api import JobDesc
from kiara_plugin.network_analysis.defaults import (
    LABEL_ALIAS_NAMES,
    NODE_ID_ALIAS_NAMES,
    SOURCE_COLUMN_ALIAS_NAMES,
    TARGET_COLUMN_ALIAS_NAMES,
)
from kiara_plugin.streamlit.components.data_import import (
    DataImportComponent,
    DataImportOptions,
)

if TYPE_CHECKING:
    from kiara_plugin.streamlit.api import KiaraStreamlitAPI


class NetworkDataImportComponent(DataImportComponent):

    _component_name = "import_network_data"
    _examples = [{"doc": "The default network_data onboarding component.", "args": {}}]  # type: ignore

    @classmethod
    def get_data_type(cls) -> str:
        return "network_data"

    def render_onboarding_page(
        self, st: "KiaraStreamlitAPI", options: DataImportOptions
    ) -> Union[None, JobDesc]:

        _key = options.create_key("import", "network_data")

        with st.expander(label="Select a table containing the edges", expanded=True):
            key = options.create_key("import", "network_data", "from", "table", "edges")
            selected_edges_table = self.get_component("select_table").render(
                st=st, key=key, add_import_widget=True
            )

        with st.expander(
            label="Select a table containing (optional) node information",
            expanded=False,
        ):
            key = options.create_key("import", "network_data", "from", "table", "nodes")
            selected_nodes_table = self.get_component("select_table").render(
                st=st, key=key, add_import_widget=True, add_no_value_option=True
            )

        with st.expander(label="Assemble options", expanded=True):
            key_column, value_column = st.columns([1, 5])
            # with key_column:
            #     st.write("Edges table")
            # with value_column:
            #     if selected_edges_table:
            #         st.kiara.preview_table(selected_edges_table, height=200)
            #     else:
            #         st.write("*-- no edges table selected --*")

            key_column, value_column = st.columns([1, 5])
            with key_column:
                st.write("Edge table options")
            with value_column:
                if selected_edges_table:
                    available_edge_coluns = selected_edges_table.data.column_names
                else:
                    available_edge_coluns = []
                edge_columns = st.columns([1, 1])
                with edge_columns[0]:
                    default = 0
                    for idx, column in enumerate(available_edge_coluns):
                        if column.lower() in SOURCE_COLUMN_ALIAS_NAMES:
                            default = idx
                            break
                    edge_source_column = st.selectbox(
                        "Source column name",
                        available_edge_coluns,
                        key=f"{_key}_edge_source_column",
                        index=default,
                    )
                with edge_columns[1]:
                    default = 0
                    for idx, column in enumerate(available_edge_coluns):
                        if column.lower() in TARGET_COLUMN_ALIAS_NAMES:
                            default = idx
                            break

                    edge_target_column = st.selectbox(
                        "Source target name",
                        available_edge_coluns,
                        key=f"{_key}_edge_target_column",
                        index=default,
                    )

            key_column, value_column = st.columns([1, 5])
            with key_column:
                st.write("Node table options")
            with value_column:
                if selected_nodes_table:
                    available_node_coluns = selected_nodes_table.data.column_names
                else:
                    available_node_coluns = []
                node_columns = st.columns([1, 1])
                with node_columns[0]:
                    default = 0
                    for idx, column in enumerate(available_node_coluns):
                        if column.lower() in NODE_ID_ALIAS_NAMES:
                            default = idx
                            break

                    node_id_column = st.selectbox(
                        "Node ID column name",
                        available_node_coluns,
                        key=f"{_key}_node_id_column",
                        index=default,
                    )
                with node_columns[1]:
                    default = 0
                    for idx, column in enumerate(available_node_coluns):
                        if column.lower() in LABEL_ALIAS_NAMES:
                            default = idx
                            break

                    label_column = st.selectbox(
                        "Label column name",
                        available_node_coluns,
                        key=f"{_key}_label_column",
                        index=default,
                    )

        if not selected_edges_table:
            return None

        inputs = {}
        inputs["edges"] = selected_edges_table.value_id
        inputs["nodes"] = (
            selected_nodes_table.value_id if selected_nodes_table else None
        )
        inputs["source_column"] = edge_source_column
        inputs["target_column"] = edge_target_column
        inputs["id_column"] = node_id_column
        inputs["label_column"] = label_column

        job_desc = {
            "operation": "assemble.network_data",
            "inputs": inputs,
            "doc": "Assemble a network_data value.",
        }
        return JobDesc(**job_desc)

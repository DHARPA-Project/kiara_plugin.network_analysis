# -*- coding: utf-8 -*-

import marimo

__generated_with = "0.14.12"
app = marimo.App(width="medium")


@app.cell
def _():
    from kiara.api import KiaraAPI
    from typing import Union
    import marimo as mo
    from kiara_plugin.network_analysis import guess_node_id_column_name, guess_node_label_column_name, guess_source_column_name, guess_target_column_name, NetworkData
    from kiara_plugin.network_analysis.utils.notebooks.marimo import prepare_altair_graph


    kiara = KiaraAPI.instance()
    kiara.set_active_context("network_analysis", create=True)
    return (
        Union,
        guess_node_id_column_name,
        guess_node_label_column_name,
        guess_source_column_name,
        guess_target_column_name,
        kiara,
        mo,
        prepare_altair_graph,
    )


@app.cell
def _(mo):
    edges_file = mo.ui.file(label="Edges file")
    edges_file
    return (edges_file,)


@app.cell
def _(mo):
    edges_file_first_row_is_header_checkbox = mo.ui.checkbox(label="First row is header")
    edges_file_first_row_is_header_checkbox
    return (edges_file_first_row_is_header_checkbox,)


@app.cell
def _(mo):
    nodes_file = mo.ui.file(label="Nodes file")
    nodes_file
    return (nodes_file,)


@app.cell
def _(mo):
    nodes_file_first_row_is_header_checkbox = mo.ui.checkbox(label="First row is header")
    nodes_file_first_row_is_header_checkbox
    return (nodes_file_first_row_is_header_checkbox,)


@app.cell
def _(Union, kiara):
    def create_table(file, table_type: str, first_row_is_header: Union[bool, None]=None):
        if file.name():
            kiara_file = kiara.run_job("create.file.from.bytes", inputs={"file_name": file.name(), "bytes": file.contents()}, comment=f"Import {table_type} data.")["file"]
            kiara_table = kiara.run_job("create.table.from.file", inputs={"file": kiara_file, "first_row_is_header": first_row_is_header}, comment=f"Create {table_type} table.")["table"]
        else:
            kiara_table = None
        return kiara_table
    return (create_table,)


@app.cell
def _(create_table, edges_file, edges_file_first_row_is_header_checkbox):
    edges_table = create_table(file=edges_file, table_type="edges", first_row_is_header=edges_file_first_row_is_header_checkbox.value)
    return (edges_table,)


@app.cell
def _(edges_table, guess_source_column_name, guess_target_column_name, mo):
    if edges_table is not None:
        pre_selected_source = guess_source_column_name(edges_table)
        options_source = edges_table.data.column_names
        text_source = "Select the column containing the edge sources"

        pre_selected_target = guess_target_column_name(edges_table)
        options_target = edges_table.data.column_names
        text_target = "Select the column containing the edge targets"
    else:
        pre_selected_source = None
        options_source = []
        text_source = "No edges imported yet"
        pre_selected_target = None
        options_target = []
        text_target = "No edges imported yet"

    source_column_name_dropdown = mo.ui.dropdown(options=options_source, value=pre_selected_source)
    target_column_name_dropdown = mo.ui.dropdown(options=options_target, value=pre_selected_target)

    edge_input = mo.vstack([mo.md(text_source), source_column_name_dropdown, mo.md(text_target), target_column_name_dropdown])
    return edge_input, source_column_name_dropdown, target_column_name_dropdown


@app.cell
def _(create_table, nodes_file, nodes_file_first_row_is_header_checkbox):
    nodes_table = create_table(file=nodes_file, table_type="nodes", first_row_is_header=nodes_file_first_row_is_header_checkbox.value)
    return (nodes_table,)


@app.cell
def _(
    guess_node_id_column_name,
    guess_node_label_column_name,
    mo,
    nodes_table,
):
    if nodes_table is not None:
        pre_selected_id = guess_node_id_column_name(nodes_table)
        options_id = nodes_table.data.column_names
        text_id = "Select the column containing the node ids"

        pre_selected_label = guess_node_label_column_name(nodes_table)
        options_label = nodes_table.data.column_names
        text_label = "Select the column containing the node labels"
    else:
        pre_selected_id = None
        options_id = []
        text_id = "No nodes imported yet"

        pre_selected_label = None
        options_label = []
        text_label = "No nodes imported yet"

    node_id_column_name_dropdown = mo.ui.dropdown(options=options_id, value=pre_selected_id)
    node_label_column_name_dropdown = mo.ui.dropdown(options=options_label, value=pre_selected_label)

    node_input = mo.vstack([mo.md(text_id), node_id_column_name_dropdown, mo.md(text_label), node_label_column_name_dropdown])
    return (
        node_id_column_name_dropdown,
        node_input,
        node_label_column_name_dropdown,
    )


@app.cell
def _(edge_input, mo, node_input):
    mo.hstack([edge_input, node_input])
    return


@app.cell
def _(
    edges_table,
    kiara,
    node_id_column_name_dropdown,
    node_label_column_name_dropdown,
    nodes_table,
    source_column_name_dropdown,
    target_column_name_dropdown,
):
    if edges_table is not None:
        node_id_column = node_id_column_name_dropdown.value
        node_label_column = node_label_column_name_dropdown.value
        source_column = source_column_name_dropdown.value
        target_column = target_column_name_dropdown.value
        assemble_inputs = {
            "edges": edges_table,
            "nodes": nodes_table,
            "id_column": node_id_column,
            "label_column": node_label_column,
            "source_column": source_column,
            "target_column": target_column
        }
        assemble_job_result = kiara.run_job("assemble.network_data", inputs=assemble_inputs, comment="Assemble network data.")
        kiara_network_data = assemble_job_result["network_data"]
    else:
        kiara_network_data = None
    return (kiara_network_data,)


@app.cell
def _(kiara_network_data, mo):
    filter_component_switch = mo.ui.switch(label="Filter component", disabled=kiara_network_data is None)
    filter_component_switch
    return (filter_component_switch,)


@app.cell
def _(filter_component_switch, kiara_network_data, mo):
    if filter_component_switch.value:
        component_options = sorted(kiara_network_data.data.component_ids)
        filter_input_dropdown = mo.ui.dropdown(options=component_options, value=0)
    else:
        filter_input_dropdown = None
    filter_input_dropdown
    return (filter_input_dropdown,)


@app.cell
def _(filter_input_dropdown, kiara, kiara_network_data):
    if filter_input_dropdown is not None:
        component_id_to_use = filter_input_dropdown.value
        filter_inputs = {
            "value": kiara_network_data,
            "component_id": component_id_to_use
        }
        filtered_network_data = kiara.run_job(
        "network_data_filter.select_component", inputs=filter_inputs, comment=f"Filter component with id {component_id_to_use}."
    )["value"]
    else:
        print("NOT")
        filtered_network_data = kiara_network_data

    return (filtered_network_data,)


@app.cell
def _(filtered_network_data, kiara_network_data, mo, prepare_altair_graph):
    if kiara_network_data is not None:
        chart = prepare_altair_graph(filtered_network_data)
        tabs = {"Graph": chart, "Edges": filtered_network_data.data.edges.arrow_table, "Nodes": filtered_network_data.data.nodes.arrow_table}
    else:
        msg = mo.md("No graph created (yet).")
        tabs = {"Graph": msg, "Edges": msg, "Nodes": msg}
    mo.ui.tabs(tabs)
    return


@app.cell
def _(filtered_network_data):
    if filtered_network_data is not None:
        properties = filtered_network_data.get_property_data("metadata.network_data")
    else:
        properties = None
    properties
    return


if __name__ == "__main__":
    app.run()

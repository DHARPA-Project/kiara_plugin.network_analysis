# -*- coding: utf-8 -*-
import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    from kiara.api import KiaraAPI
    from kiara.interfaces.cli import terminal_print
    import marimo as mo
    import rustworkx as rx
    from rustworkx.visualization import graphviz_draw, mpl_draw
    import matplotlib.pyplot as plt
    from kiara_plugin.network_analysis.utils.notebooks.marimo import prepare_mpl_graph, prepare_altair_graph, prepare_plotly_graph
    from kiara_plugin.network_analysis.defaults import LABEL_COLUMN_NAME


    kiara = KiaraAPI.instance()
    kiara.set_active_context("network_analysis", create=True)
    return (
        kiara,
        mo,
        prepare_altair_graph,
        prepare_mpl_graph,
        prepare_plotly_graph,
    )


@app.cell
def _(mo):
    edges_file = mo.ui.file(label="Edges file")
    edges_file
    return (edges_file,)


@app.cell
def _(mo):
    nodes_file = mo.ui.file(label="Nodes file")
    nodes_file
    return (nodes_file,)


@app.cell
def _(kiara):
    def create_table(file):
        if file.name():
            kiara_file = kiara.run_job("create.file.from.bytes", inputs={"file_name": file.name(), "bytes": file.contents()})["file"]
            kiara_table = kiara.run_job("create.table.from.file", inputs={"file": kiara_file})["table"]
        else:
            kiara_table = None
        return kiara_table
    return (create_table,)


@app.cell
def _(create_table, edges_file, nodes_file):
    nodes_table = create_table(file=nodes_file)
    if nodes_table is not None:
        print("Nodes table set!")
    else:
        print("Nodes table not set!")


    edges_table = create_table(file=edges_file)
    if edges_table is not None:
        print("Edges table set!")
    else:
        print("Edges table not set!")
    return edges_table, nodes_table


@app.cell
def _(edges_table, kiara, nodes_table):
    if edges_table is not None:
        assemble_inputs = {
            "edges": edges_table,
            "nodes": nodes_table
        }
        kiara_network_data = kiara.run_job("assemble.network_data", inputs=assemble_inputs)["network_data"]
    else:
        kiara_network_data = None
    return (kiara_network_data,)


@app.cell
def _(kiara_network_data, prepare_mpl_graph):
    chart_mpl = prepare_mpl_graph(kiara_network_data)
    chart_mpl
    return


@app.cell
def _(kiara_network_data, prepare_plotly_graph):
    fig = prepare_plotly_graph(kiara_network_data)
    fig
    return


@app.cell
def _(kiara_network_data, prepare_altair_graph):
    chart = prepare_altair_graph(kiara_network_data)

    chart
    return


if __name__ == "__main__":
    app.run()

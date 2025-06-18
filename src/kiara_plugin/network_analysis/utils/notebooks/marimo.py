# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Union

import marimo as mo
import rustworkx as rx

from kiara_plugin.network_analysis.defaults import LABEL_COLUMN_NAME
from kiara_plugin.network_analysis.utils import extract_network_data

if TYPE_CHECKING:
    from kiara.models.values.value import Value
    from kiara_plugin.network_analysis.models import NetworkData


def prepare_mpl_graph(network_data: Union["Value", "NetworkData", None]):
    import matplotlib.pyplot as plt
    import rustworkx as rx
    from rustworkx.visualization import mpl_draw

    plt.figure()

    if network_data is not None:
        network_data = extract_network_data(network_data)
        graph = network_data.as_rustworkx_graph(
            rx.PyGraph, incl_node_attributes=[LABEL_COLUMN_NAME]
        )
        mpl_draw(graph, with_labels=True, labels=lambda node: node[LABEL_COLUMN_NAME])  # type: ignore
    else:
        # Create an empty plot or placeholder
        plt.text(
            0.5,
            0.5,
            "No graph data available",
            horizontalalignment="center",
            verticalalignment="center",
        )

    chart_mpl = mo.mpl.interactive(plt.gcf())
    return chart_mpl


def prepare_altair_graph(network_data: Union["Value", "NetworkData", None]):
    import altair as alt
    import pandas as pd

    if network_data is None:
        return None

    network_data = extract_network_data(network_data)
    # Get the rustworkx graph with node attributes (including labels)
    rx_graph = network_data.as_rustworkx_graph(rx.PyGraph, incl_node_attributes=True)

    # Get node positions using spring layout
    pos = rx.spring_layout(rx_graph, seed=42)

    # Create nodes dataframe
    nodes_data = []
    for node_idx in rx_graph.node_indices():
        node_data = rx_graph[node_idx]
        label = node_data.get("_label", f"Node {node_idx}")
        degree = len(list(rx_graph.neighbors(node_idx)))

        nodes_data.append(
            {
                "node_id": node_idx,
                "x": pos[node_idx][0],
                "y": pos[node_idx][1],
                "label": label,
                "degree": degree,
                "original_id": node_data.get("_node_id", node_idx),
            }
        )

    nodes_df = pd.DataFrame(nodes_data)

    # Create edges dataframe
    edges_data = []
    for edge in rx_graph.edge_list():
        source_pos = pos[edge[0]]
        target_pos = pos[edge[1]]

        edges_data.append(
            {
                "source": edge[0],
                "target": edge[1],
                "x": source_pos[0],
                "y": source_pos[1],
                "x2": target_pos[0],
                "y2": target_pos[1],
            }
        )

    edges_df = pd.DataFrame(edges_data)

    # Create edges layer
    edges_chart = (
        alt.Chart(edges_df)
        .mark_rule(color="lightgray", strokeWidth=1, opacity=0.6)
        .encode(
            x=alt.X("x:Q", scale=alt.Scale(nice=False), axis=None),
            y=alt.Y("y:Q", scale=alt.Scale(nice=False), axis=None),
            x2="x2:Q",
            y2="y2:Q",
        )
    )

    # Create nodes layer with selection
    click_selection = alt.selection_point(fields=["node_id"], toggle=True)

    nodes_chart = (
        alt.Chart(nodes_df)
        .mark_circle(size=100, stroke="white", strokeWidth=2)
        .encode(
            x=alt.X("x:Q", scale=alt.Scale(nice=False), axis=None),
            y=alt.Y("y:Q", scale=alt.Scale(nice=False), axis=None),
            color=alt.Color(
                "degree:Q",
                scale=alt.Scale(scheme="viridis"),
                legend=alt.Legend(title="Node Degree"),
            ),
            size=alt.Size(
                "degree:Q",
                scale=alt.Scale(range=[50, 200]),
                legend=alt.Legend(title="Node Degree"),
            ),
            opacity=alt.condition(click_selection, alt.value(1.0), alt.value(0.7)),
            stroke=alt.condition(click_selection, alt.value("red"), alt.value("white")),
            strokeWidth=alt.condition(click_selection, alt.value(3), alt.value(1)),
            tooltip=["label:N", "original_id:O", "degree:Q"],
        )
        .add_params(click_selection)
    )

    # Combine layers
    chart = (
        (edges_chart + nodes_chart)
        .resolve_scale(color="independent")
        .properties(
            width=600,
            height=400,
            title="Interactive Network Graph (RustWorkX + Altair)",
        )
        .interactive()
    )

    # Use marimo's altair chart with selection support
    chart = mo.ui.altair_chart(chart)

    return chart


def prepare_plotly_graph(network_data: Union["Value", "NetworkData", None]):
    import plotly.graph_objects as go
    import plotly.io as pio

    if network_data is None:
        return None

    # Set plotly renderer for marimo compatibility
    pio.renderers.default = "json"

    # Get the rustworkx graph
    network_data = extract_network_data(network_data)
    rx_graph = network_data.as_rustworkx_graph(
        rx.PyGraph, incl_node_attributes=[LABEL_COLUMN_NAME]
    )

    # Get node positions using spring layout
    pos = rx.spring_layout(rx_graph, seed=42)

    # Extract node coordinates
    node_x = [pos[node][0] for node in rx_graph.node_indices()]
    node_y = [pos[node][1] for node in rx_graph.node_indices()]

    # Get node labels/info
    node_info = [
        rx_graph.get_node_data(i)[LABEL_COLUMN_NAME] for i in rx_graph.node_indices()
    ]

    # Extract edge coordinates
    edge_x = []
    edge_y = []
    for edge in rx_graph.edge_list():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    # Create edge trace
    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line={"width": 0.5, "color": "#888"},
        hoverinfo="none",
        mode="lines",
    )

    # Create node trace
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode="markers",
        hoverinfo="text",
        text=node_info,
        marker={
            "showscale": True,
            "colorscale": "YlGnBu",
            "reversescale": True,
            "color": [],
            "size": 10,
            "colorbar": {
                "thickness": 15,
                "len": 0.5,
                "x": 1.02,
                "title": "Node Connections",
            },
            "line": {"width": 2},
        },
    )

    # Color nodes by number of connections
    node_adjacencies = []
    for node in rx_graph.node_indices():
        adjacencies = len(list(rx_graph.neighbors(node)))
        node_adjacencies.append(adjacencies)

    node_trace.marker.color = node_adjacencies

    # Create the figure
    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            title={
                "text": "Interactive Network Graph (RustWorkX + Plotly)",
                "font": {"size": 16},
            },
            showlegend=False,
            hovermode="closest",
            margin={"b": 20, "l": 5, "r": 5, "t": 40},
            annotations=[
                {
                    "text": "Hover over nodes to see details",
                    "showarrow": False,
                    "xref": "paper",
                    "yref": "paper",
                    "x": 0.005,
                    "y": -0.002,
                    "xanchor": "left",
                    "yanchor": "bottom",
                    "font": {"color": "#888", "size": 12},
                }
            ],
            xaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
            yaxis={"showgrid": False, "zeroline": False, "showticklabels": False},
        ),
    )

    return fig

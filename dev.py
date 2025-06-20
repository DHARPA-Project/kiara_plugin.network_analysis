# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Union

from kiara.interfaces.python_api.kiara_api import KiaraAPI
from kiara.models.values.value import Value

kiara = KiaraAPI.instance()


def create_table(path: Union[str, Path, None]) -> Union[str, Value]:
    if path is None:
        return None
    elif isinstance(path, Path):
        path = path.as_posix()

    kiara_file = kiara.run_job("import.local.file", inputs={"path": path})["file"]
    kiara_table = kiara.run_job("create.table.from.file", inputs={"file": kiara_file})[
        "table"
    ]

    return kiara_table


edges_file_path = "/home/markus/projects/kiara/plugins/kiara_plugin.network_analysis/examples/data/journals/JournalEdges1902.csv"
nodes_file_path = "/home/markus/projects/kiara/plugins/kiara_plugin.network_analysis/examples/data/journals/JournalNodes1902.csv"

# edges_file_path = "/home/markus/projects/kiara/plugins/kiara_plugin.network_analysis/examples/data/simple_networks/two_components/SampleEdges.csv"
# nodes_file_path = "/home/markus/projects/kiara/plugins/kiara_plugin.network_analysis/examples/data/simple_networks/two_components/SampleNodes.csv"

# edges_file_path = "/home/markus/projects/kiara/plugins/kiara_plugin.network_analysis/examples/data/simple_networks/connected/SampleEdges.csv"
# nodes_file_path = "/home/markus/projects/kiara/plugins/kiara_plugin.network_analysis/examples/data/simple_networks/connected/SampleNodes.csv"

edges_table = create_table(edges_file_path)
nodes_table = create_table(nodes_file_path)

assemble_inputs = {"edges": edges_table, "nodes": nodes_table}
kiara_network_data = kiara.run_job("assemble.network_data", inputs=assemble_inputs)[
    "network_data"
]

filter_inputs = {"value": kiara_network_data, "component_id": 0}
filtered_network_data = kiara.run_job(
    "network_data_filter.select_component", inputs=filter_inputs
)["value"]

dbg(filtered_network_data)

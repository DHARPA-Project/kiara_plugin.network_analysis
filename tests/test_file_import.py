# -*- coding: utf-8 -*-
import os
from pathlib import Path

from kiara.interfaces.python_api.kiara_api import KiaraAPI

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
EXAMPLES_DIR = Path(os.path.join(ROOT_DIR, "examples"))
DATA_DIR = Path(os.path.join(EXAMPLES_DIR, "data"))


def test_wikipedia_data(kiara_api: KiaraAPI):
    edges_csv = DATA_DIR / "wikipedia" / "wikipedia_sample_edges.csv"

    job_desc = {
        "operation": "create.table.from.file",
        "inputs": {
            "file": edges_csv.as_posix(),
        },
        "comment": "Import wikipedia sample edges csv.",
    }

    edges_table = kiara_api.run_job(**job_desc)["table"]

    nodes_csv = DATA_DIR / "wikipedia" / "wikipedia_sample_nodes.csv"
    job_desc = {
        "operation": "create.table.from.file",
        "inputs": {
            "file": nodes_csv.as_posix(),
        },
        "comment": "Import wikipedia sample nodes csv.",
    }

    nodes_table = kiara_api.run_job(**job_desc)["table"]

    # dbg(edges_table)
    # dbg(nodes_table)

    result_network = kiara_api.run_job(
        operation="assemble.network_data",
        inputs={
            "edges": edges_table,
            "nodes": nodes_table,
            "source_column": "Source",
            "target_column": "Target",
        },
        comment="Create network data from wikipedia sample.",
    )

    nd_value = result_network["network_data"]
    assert nd_value.data.num_nodes == 1402
    assert nd_value.data.num_edges == 1315

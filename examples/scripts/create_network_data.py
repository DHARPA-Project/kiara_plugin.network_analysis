# -*- coding: utf-8 -*-
from kiara.interfaces.python_api.workflow import Workflow
from kiara.utils.cli import terminal_print_model

workflow = Workflow.create("network_analysis")

workflow.add_step(operation="import.file", step_id="import_edges")
workflow.add_step(operation="create.table.from.csv_file", step_id="create_edges_table")
workflow.add_step(operation="import.file", step_id="import_nodes")
workflow.add_step(operation="create.table.from.csv_file", step_id="create_nodes_table")
workflow.add_step(
    operation="create.network_data.from.tables", step_id="assemble_network_data"
)

workflow.connect_steps("import_edges", "file", "create_edges_table", "csv_file")
workflow.connect_steps("import_nodes", "file", "create_nodes_table", "csv_file")
workflow.connect_steps("create_edges_table", "table", "assemble_network_data", "edges")
workflow.connect_steps("create_nodes_table", "table", "assemble_network_data", "nodes")

inputs = {
    "import_edges__path": "/home/markus/projects/network_analysis/kiara_plugin.network_analysis/examples/data/journals/JournalEdges1902.csv",
    "import_nodes__path": "/home/markus/projects/network_analysis/kiara_plugin.network_analysis/examples/data/journals/JournalNodes1902.csv",
    "assemble_network_data__source_column_name": "Source",
    "assemble_network_data__target_column_name": "Target",
    "assemble_network_data__id_column_name": "Id",
    "assemble_network_data__label_column_name": "Label",
}
workflow.set_inputs(**inputs)

workflow.process_steps()

terminal_print_model(workflow)

# -*- coding: utf-8 -*-
import os

from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara_plugin.network_analysis.models import NetworkData

KIARA_METADATA = {
    "authors": [{"name": "Markus Binsteiner", "email": "markus@frkl.io"}],
    "description": "Modules related to extracting components from network data.",
}


class ExportNetworkDataModule(DataExportModule):
    """Export network data items."""

    _module_type_name = "export.network_data"

    # def export__network_data__as__graphml_file(
    #     self, value: NetworkData, base_path: str, name: str
    # ):
    #     """Export network data as graphml file."""
    #
    #     import networkx as nx
    #
    #     target_path = os.path.join(base_path, f"{name}.graphml")
    #
    #     # TODO: can't just assume digraph
    #     graph: nx.Graph = value.as_networkx_graph(nx.DiGraph)
    #     nx.write_graphml(graph, target_path)
    #
    #     return {"files": target_path}
    #
    # def export__network_data__as__sqlite_db(
    #     self, value: NetworkData, base_path: str, name: str
    # ):
    #     """Export network data as a sqlite database file."""
    #
    #     target_path = os.path.abspath(os.path.join(base_path, f"{name}.sqlite"))
    #     shutil.copy2(value.db_file_path, target_path)
    #
    #     return {"files": target_path}
    #
    # def export__network_data__as__sql_dump(
    #     self, value: NetworkData, base_path: str, name: str
    # ):
    #     """Export network data as a sql dump file."""
    #
    #     import sqlite_utils
    #
    #     db = sqlite_utils.Database(value.db_file_path)
    #     target_path = Path(os.path.join(base_path, f"{name}.sql"))
    #     with target_path.open("wt") as f:
    #         for line in db.conn.iterdump():
    #             f.write(line + "\n")
    #
    #     return {"files": target_path.as_posix()}

    def export__network_data__as__csv_files(
        self, value: NetworkData, base_path: str, name: str
    ):
        """Export network data as 2 csv files (one for edges, one for nodes."""

        from pyarrow import csv

        files = []

        for table_name in value.table_names:
            target_path = os.path.join(base_path, f"{name}__{table_name}.csv")
            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            table = value.get_table(table_name)

            csv.write_csv(table.arrow_table, target_path)
            files.append(target_path)

        return {"files": files}

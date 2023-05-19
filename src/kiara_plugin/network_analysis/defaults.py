# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from enum import Enum

ID_COLUMN_NAME = "_id"
SOURCE_COLUMN_NAME = "_source"
TARGET_COLUMN_NAME = "_target"
LABEL_COLUMN_NAME = "_label"

DEFAULT_NETWORK_DATA_CHUNK_SIZE = 1024

NODE_ID_ALIAS_NAMES = ["id", "node_id"]
LABEL_ALIAS_NAMES = ["label", "node_label"]

SOURCE_COLUMN_ALIAS_NAMES = ["source", "sources", "source_id", "from", "sender"]
TARGET_COLUMN_ALIAS_NAMES = ["target", "targets", "target_id", "to", "receiver"]

# WEIGHT_COLUMN_ALIAS_NAMES = [
#     "weight",
#     "weights",
#     "edge_weight",
#     "edge_weights",
#     "strength",
#     "strengths",
# ]

WEIGHT_DIRECTED_COLUMN_NAME = "_dup_directed"
WEIGHT_UNDIRECTED_COLUMN_NAME = "_dup_undirected"

RANKING_TABLE_NAME = "ranking"
RANKING_COLUNN_NAME = "_rank"
RANKING_VALUE_COLUMN_NAME = "_value"


class NetworkDataTableType(Enum):
    EDGES = "edges"
    NODES = "nodes"

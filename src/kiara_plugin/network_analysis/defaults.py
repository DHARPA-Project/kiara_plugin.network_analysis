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


class NetworkDataTableType(Enum):

    EDGES = "edges"
    NODES = "nodes"

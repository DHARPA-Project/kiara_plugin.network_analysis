# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
from enum import Enum

ID_COLUMN_NAME = "id"
SOURCE_COLUMN_NAME = "source"
TARGET_COLUMN_NAME = "target"
LABEL_COLUMN_NAME = "label"

DEFAULT_NETWORK_DATA_CHUNK_SIZE = 1024


class NetworkDataTableType(Enum):

    EDGES = "edges"
    NODES = "nodes"

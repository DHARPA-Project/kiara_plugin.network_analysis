# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
import sys
from enum import Enum

if not hasattr(sys, "frozen"):
    KIARA_PLUGIN_NETWORK_BASE_FOLDER = os.path.dirname(__file__)
    """Marker to indicate the base folder for the kiara network module package."""
else:
    KIARA_PLUGIN_NETWORK_BASE_FOLDER = os.path.join(sys._MEIPASS, os.path.join("kiara_modules", "network_analysis"))  # type: ignore
    """Marker to indicate the base folder for the kiara network module package."""

KIARA_PLUGIN_NETWORK_RESOURCES_FOLDER = os.path.join(
    KIARA_PLUGIN_NETWORK_BASE_FOLDER, "resources"
)
"""Default resources folder for this package."""

TEMPLATES_FOLDER = os.path.join(KIARA_PLUGIN_NETWORK_RESOURCES_FOLDER, "templates")

ID_COLUMN_NAME = "id"
SOURCE_COLUMN_NAME = "source"
TARGET_COLUMN_NAME = "target"
LABEL_COLUMN_NAME = "label"

DEFAULT_NETWORK_DATA_CHUNK_SIZE = 1024


class NetworkDataTableType(Enum):

    EDGES = "edges"
    NODES = "nodes"

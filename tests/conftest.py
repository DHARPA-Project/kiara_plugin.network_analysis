#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Dummy conftest.py for kiara_plugin.language_processing.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    https://pytest.org/latest/plugins.html
"""
import os
import tempfile
import uuid
from pathlib import Path

import pytest
from kiara.context import KiaraConfig
from kiara.interfaces.python_api import KiaraAPI

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_FOLDER = Path(os.path.join(ROOT_DIR, "examples", "data"))


def create_temp_dir():
    session_id = str(uuid.uuid4())
    TEMP_DIR = Path(os.path.join(tempfile.gettempdir(), "kiara_tests"))

    instance_path = os.path.join(
        TEMP_DIR.resolve().absolute(), f"instance_{session_id}"
    )
    return instance_path


@pytest.fixture
def kiara_api() -> KiaraAPI:

    instance_path = create_temp_dir()
    kc = KiaraConfig.create_in_folder(instance_path)
    api = KiaraAPI(kc)
    return api


@pytest.fixture
def data_folder() -> Path:
    return DATA_FOLDER

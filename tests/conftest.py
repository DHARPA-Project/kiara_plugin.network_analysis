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
from kiara.interfaces.python_api.models.job import JobTest
from kiara.utils.testing import get_tests_for_job, list_job_descs

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
JOBS_FOLDER = Path(os.path.join(ROOT_DIR, "examples", "jobs"))


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


@pytest.fixture(params=list_job_descs(JOBS_FOLDER))
def example_job_test(request, kiara_api) -> JobTest:

    job_tests_folder = Path(os.path.join(ROOT_DIR, "tests", "job_tests"))

    job_desc = request.param
    tests = get_tests_for_job(
        job_alias=job_desc.job_alias, job_tests_folder=job_tests_folder
    )

    job_test = JobTest(kiara_api=kiara_api, job_desc=job_desc, tests=tests)
    return job_test


@pytest.fixture
def example_data_folder() -> Path:
    return Path(os.path.join(ROOT_DIR, "examples", "data"))


@pytest.fixture
def example_pipelines_folder() -> Path:
    return Path(os.path.join(ROOT_DIR, "examples", "pipelines"))


@pytest.fixture()
def tests_resources_folder() -> Path:
    return Path(os.path.join(ROOT_DIR, "tests"))

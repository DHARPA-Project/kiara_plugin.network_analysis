#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dummy conftest.py for kiara_plugin.language_processing.

If you don't know what this is for, just leave it empty.
Read more about conftest.py under:
https://pytest.org/latest/plugins.html
"""
# import pytest

import os
import tempfile
import uuid
from pathlib import Path

import pytest
from _pytest.compat import NotSetType

from kiara.api import JobDesc, KiaraAPI
from kiara.context import KiaraConfig
from kiara.interfaces.python_api.base_api import BaseAPI
from kiara.interfaces.python_api.models.job import JobTest
from kiara.utils.testing import get_init_job, get_tests_for_job, list_job_descs

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
JOBS_FOLDERS = [
    Path(os.path.join(ROOT_DIR, "tests", "resources", "jobs")),
    Path(os.path.join(ROOT_DIR, "examples", "jobs")),
]


def create_temp_dir():
    session_id = str(uuid.uuid4())
    TEMP_DIR = Path(os.path.join(tempfile.gettempdir(), "kiara_tests"))

    instance_path = os.path.join(
        TEMP_DIR.resolve().absolute(), f"instance_{session_id}"
    )
    return instance_path


def get_job_alias(job_desc: JobDesc) -> str:
    if isinstance(job_desc, NotSetType):
        return "-- no example jobs --"

    return job_desc.job_alias


@pytest.fixture
def kiara_api() -> BaseAPI:
    instance_path = create_temp_dir()
    kc = KiaraConfig.create_in_folder(instance_path)
    api = BaseAPI(kc)
    return api


@pytest.fixture
def kiara_api_init_example() -> KiaraAPI:
    instance_path = create_temp_dir()
    kc = KiaraConfig.create_in_folder(instance_path)
    api = KiaraAPI(kc)

    init_jobs = []
    for jobs_folder in JOBS_FOLDERS:
        init_job = get_init_job(jobs_folder)
        if init_job is not None:
            init_jobs.append(init_job)

    if not init_jobs:
        return api

    for init_job in init_jobs:
        results = api.run_job(init_job, comment="Init example job")

        if not init_job.save:
            continue

        for field_name, alias_name in init_job.save.items():
            api.store_value(results[field_name], alias_name)

    return api


@pytest.fixture(params=list_job_descs(JOBS_FOLDERS), ids=get_job_alias)
def example_job_test(request, kiara_api_init_example) -> JobTest:
    job_tests_folder = Path(os.path.join(ROOT_DIR, "tests", "job_tests"))

    job_desc = request.param
    tests = get_tests_for_job(
        job_alias=job_desc.job_alias, job_tests_folder=job_tests_folder
    )

    job_test = JobTest(kiara_api=kiara_api_init_example, job_desc=job_desc, tests=tests)
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

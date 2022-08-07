# -*- coding: utf-8 -*-
import os

import mkdocs_gen_files
from kiara.context import Kiara

kiara = Kiara.instance()

modules_file_path = os.path.join("modules_list.md")
modules_page_content = """# Available module types

This page contains a list of all available *Kiara* module types, and their details.

!!! note
The formatting here will be improved later on, for now this should be enough to get the important details of each module type.

"""

BASE_PACKAGE = "kiara_plugin.network_analysis"


for module_type in kiara.module_mgmt.find_modules_for_package(
    BASE_PACKAGE, include_pipelines=False
).keys():

    if module_type == "pipeline":
        continue

    modules_page_content = modules_page_content + f"## ``{module_type}``\n\n"
    modules_page_content = (
        modules_page_content
        + "```\n{{ get_module_info('"
        + module_type
        + "') }}\n```\n\n"
    )

with mkdocs_gen_files.open(modules_file_path, "w") as f:
    f.write(modules_page_content)

pipelines_file_path = os.path.join("pipelines_list.md")
pipelines_page_content = """# Available pipeline module types

This page contains a list of all available *Kiara* pipeline module types, and their details.

!!! note
The formatting here will be improved later on, for now this should be enough to get the important details of each module type.

"""

for module_type in kiara.module_mgmt.find_modules_for_package(
    BASE_PACKAGE, include_core_modules=False
):

    if module_type == "pipeline":
        continue

    pipelines_page_content = pipelines_page_content + f"## ``{module_type}``\n\n"
    pipelines_page_content = (
        pipelines_page_content
        + "```\n{{ get_module_info('"
        + module_type
        + "') }}\n```\n\n"
    )

with mkdocs_gen_files.open(pipelines_file_path, "w") as f:
    f.write(pipelines_page_content)

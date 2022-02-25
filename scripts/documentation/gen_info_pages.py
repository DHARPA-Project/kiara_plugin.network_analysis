# -*- coding: utf-8 -*-
import mkdocs_gen_files
from kiara import Kiara
from kiara.doc.gen_info_pages import generate_pages_and_summary_for_types

pkg_name = "kiara_modules.network_analysis"
kiara = Kiara.instance()

value_types = kiara.type_mgmt.find_value_types_for_package(pkg_name)
modules = kiara.module_mgmt.find_modules_for_package(
    pkg_name, include_core_modules=True, include_pipelines=False
)
pipelines = kiara.module_mgmt.find_modules_for_package(
    pkg_name, include_core_modules=False, include_pipelines=True
)
operation_types = kiara.operation_mgmt.find_operation_types_for_package(pkg_name)

types = []
if value_types:
    types.append("value_type")
if modules:
    types.append("module")
if pipelines:
    types.append("pipelines")
if operation_types:
    types.append("operation_type")

type_details = generate_pages_and_summary_for_types(
    kiara=kiara,
    types=types,
    limit_to_package="kiara_modules.network_analysis",
)

summary_content = []
for name, details in type_details.items():
    line = f"* [{details['name']}]({details['path']})"
    summary_content.append(line)


nav = ["* [Home](index.md)", "* [Usage](usage.md)", "* [Development](development.md)"]
nav.extend(summary_content)

nav.append("* [API docs](reference/)")

with mkdocs_gen_files.open("SUMMARY.md", "w") as f:
    f.write("\n".join(nav))

# -*- coding: utf-8 -*-

"""Top-level package for kiara_plugin.network_analysis."""


import os

from kiara.utils.class_loading import (
    KiaraEntryPointItem,
    find_data_types_under,
    find_kiara_model_classes_under,
    find_kiara_modules_under,
    find_pipeline_base_path_for_module,
)

__author__ = """Markus Binsteiner"""
__email__ = "markus@frkl.io"

KIARA_METADATA = {
    "authors": [],
    "description": "Kiara modules for: network_analysis",
    "references": {
        "source_repo": {
            "desc": "The module package git repository.",
            "url": "https://github.com/DHARPA-Project/kiara_plugin.network_analysis",
        },
        "documentation": {
            "desc": "The url for the module package documentation.",
            "url": "https://DHARPA-Project.github.io/kiara_plugin.network_analysis/",
        },
    },
    "tags": ["network_analysis"],
    "labels": {"package": "kiara_plugin.network_analysis"},
}

find_modules: KiaraEntryPointItem = (
    find_kiara_modules_under,
    "kiara_plugin.network_analysis.modules",
)
find_model_classes: KiaraEntryPointItem = (
    find_kiara_model_classes_under,
    "kiara_plugin.network_analysis.models",
)
find_data_types: KiaraEntryPointItem = (
    find_data_types_under,
    "kiara_plugin.network_analysis.data_types",
)
find_pipelines: KiaraEntryPointItem = (
    find_pipeline_base_path_for_module,
    "kiara_plugin.network_analysis.pipelines",
    KIARA_METADATA,
)

try:
    from kiara_plugin.streamlit import find_kiara_streamlit_components_under

    find_kiara_streamlit_components: KiaraEntryPointItem = (
        find_kiara_streamlit_components_under,
        "kiara_plugin.network_analysis.streamlit.components",
    )
except Exception:
    find_kiara_streamlit_components = list


def get_version():
    from importlib.metadata import PackageNotFoundError, version

    try:
        # Change here if project is renamed and does not equal the package name
        dist_name = __name__
        __version__ = version(dist_name)
    except PackageNotFoundError:

        try:
            version_file = os.path.join(os.path.dirname(__file__), "version.txt")

            if os.path.exists(version_file):
                with open(version_file, encoding="utf-8") as vf:
                    __version__ = vf.read()
            else:
                __version__ = "unknown"

        except (Exception):
            pass

        if __version__ is None:
            __version__ = "unknown"

    return __version__

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
from kiara_plugin.network_analysis.data_types import NetworkDataType
from kiara_plugin.network_analysis.models import NetworkData
from kiara_plugin.network_analysis.utils import (
    guess_node_id_column_name,
    guess_node_label_column_name,
    guess_source_column_name,
    guess_target_column_name,
)

__all__ = [
    "get_version",
    "guess_node_id_column_name",
    "guess_node_label_column_name",
    "guess_source_column_name",
    "guess_target_column_name",
    "NetworkData",
    "NetworkDataType",
]

__author__ = """Markus Binsteiner"""
__email__ = "markus@frkl.dev"


KIARA_METADATA = {
    "authors": [{"name": __author__, "email": __email__}],
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
"""Kiara metadata for the `kiara_plugin.network_analysis` module."""


find_modules: KiaraEntryPointItem = (
    find_kiara_modules_under,
    "kiara_plugin.network_analysis.modules",
)
"""Entry point to discover all `kiara` modules for this plugin."""

find_model_classes: KiaraEntryPointItem = (
    find_kiara_model_classes_under,
    "kiara_plugin.network_analysis.models",
)
"""Entry point to discover all `kiara` model classes for this plugin."""

find_data_types: KiaraEntryPointItem = (
    find_data_types_under,
    "kiara_plugin.network_analysis.data_types",
)
"""Entry point to discover all `kiara` data types for this plugin."""
find_pipelines: KiaraEntryPointItem = (
    find_pipeline_base_path_for_module,
    "kiara_plugin.network_analysis.pipelines",
    KIARA_METADATA,
)
"""Entry point to discover all `kiara` pipelines for this plugin."""


def get_version() -> str:
    """Get the current version of the `kiara_plugin.network_analysis` module.

    This tries to get the version from the current git commit or tag, if possible.

    Returns:
        str: The version string.

    """

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

        except Exception:
            pass

        if __version__ is None:
            __version__ = "unknown"

    return __version__

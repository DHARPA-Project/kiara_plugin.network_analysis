# -*- coding: utf-8 -*-

"""Top-level package for kiara_modules.network_analysis."""

__all__ = [
    "AugmentNetworkGraphModule",
    "CreateGraphFromEdgesTableModule",
    "AddNodesToNetworkGraphModule",
    "ExtractGraphPropertiesModule",
    "FindShortestPathModule",
    "get_version",
]


import logging
import os

from kiara import KiaraEntryPointItem, find_kiara_modules_under

from kiara_modules.network_analysis.graph import (
    AddNodesToNetworkGraphModule,
    AugmentNetworkGraphModule,
    CreateGraphFromEdgesTableModule,
    ExtractGraphPropertiesModule,
    FindShortestPathModule,
)

__author__ = """Markus Binsteiner"""
__email__ = "markus@frkl.io"

log = logging.getLogger("kiara_modules")

KIARA_METADATA = {
    "authors": [{"name": __author__, "email": __email__}],
    "description": "Kiara modules for: network-analysis",
    "references": {
        "source_repo": {
            "desc": "The module package git repository.",
            "url": "https://github.com/DHARPA-Project/kiara_modules.network_analysis",
        },
        "documentation": {
            "desc": "The url for the module package documentation.",
            "url": "https://dharpa.org/kiara_modules.network_analysis/",
        },
    },
    "tags": ["network_analysis"],
    "labels": {"package": "kiara_modules.network_analysis"},
}


modules: KiaraEntryPointItem = (
    find_kiara_modules_under,
    ["kiara_modules.network_analysis"],
)


def get_version():
    from pkg_resources import DistributionNotFound, get_distribution

    try:
        # Change here if project is renamed and does not equal the package name
        dist_name = __name__
        __version__ = get_distribution(dist_name).version
    except DistributionNotFound:

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

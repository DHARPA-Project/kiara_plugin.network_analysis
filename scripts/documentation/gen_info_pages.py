# -*- coding: utf-8 -*-
#  Copyright (c) 2022-2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)

import builtins

from kiara.context import Kiara, KiaraContextInfo
from kiara.doc.gen_info_pages import generate_detail_pages

pkg_name = "kiara_plugin.network_analysis"

kiara: Kiara = Kiara.instance()
context_info = KiaraContextInfo.create_from_kiara_instance(
    kiara=kiara, package_filter=pkg_name
)

generate_detail_pages(context_info=context_info)

builtins.plugin_package_context_info = context_info

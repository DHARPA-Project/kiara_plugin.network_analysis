# -*- coding: utf-8 -*-
# from kiara.models.rendering import RenderValueResult
# from kiara.modules.included_core_modules.render_value import RenderValueModule
#
# KIARA_METADATA = {
#     "authors": [
#        {"name": "Markus Binsteiner", "email": "markus@frkl.io"}
#     ],
#     "description": "Modules related to extracting components from network data.",
# }
# class RenderNetworkModule(RenderValueModule):
#     _module_type_name = "render.network_data.for.web"
#
#     def render__network_data__as__html(
#         self, value: Value, render_config: Mapping[str, Any]
#     ):
#         input_number_of_rows = render_config.get("number_of_rows", 20)
#         input_row_offset = render_config.get("row_offset", 0)
#
#         table_name = render_config.get("table_name", None)
#
#         wrap, data_related_scenes = self.preprocess_database(
#             value=value,
#             table_name=table_name,
#             input_number_of_rows=input_number_of_rows,
#             input_row_offset=input_row_offset,
#         )
#         pretty = wrap.as_html(max_row_height=1)
#
#         result = RenderValueResult(
#             value_id=value.value_id,
#             render_config=render_config,
#             render_manifest=self.manifest.manifest_hash,
#             rendered=pretty,
#             related_scenes=data_related_scenes,
#         )
#         return result

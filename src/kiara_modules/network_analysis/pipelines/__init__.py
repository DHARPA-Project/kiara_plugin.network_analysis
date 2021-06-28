# -*- coding: utf-8 -*-

"""Virtual module that is used as base for [PipelineModule][kiara.pipeline.module.PipelineModule] classes that are auto-generated
from pipeline descriptions under this folder."""
from kiara import KiaraEntryPointItem, find_pipeline_base_path_for_module

pipelines: KiaraEntryPointItem = (
    find_pipeline_base_path_for_module,
    ["kiara_modules.network_analysis.pipelines"],
)

KIARA_METADATA = {"tags": ["pipeline"], "labels": {"pipeline": "true"}}

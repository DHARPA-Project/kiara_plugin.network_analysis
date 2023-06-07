# -*- coding: utf-8 -*-
from pydantic import Field, validator

from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara_plugin.network_analysis.defaults import (
    EDGE_SOURCE_TEXT,
    EDGE_TARGET_TEXT,
    NODE_ID_TEXT,
    NODE_LABEL_TEXT,
)


class NetworkNodeAttributeMetadata(KiaraModel):

    _kiara_model_id = "metadata.network_node_attribute"

    doc: DocumentationMetadataModel = Field(
        description="Explanation what this attribute is about.",
        default_factory=DocumentationMetadataModel.create,
    )

    @validator("doc", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)


class NetworkEdgeAttributeMetadata(KiaraModel):

    _kiara_model_id = "metadata.network_edge_attribute"

    doc: DocumentationMetadataModel = Field(
        description="Explanation what this attribute is about.",
        default_factory=DocumentationMetadataModel.create,
    )

    @validator("doc", pre=True)
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)


NODE_ID_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_ID_TEXT)  # type: ignore
NODE_LABEL_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_LABEL_TEXT)  # type: ignore

EDGE_ID_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc="The unique id for the edge.")  # type: ignore
EDGE_SOURCE_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc=EDGE_SOURCE_TEXT)  # type: ignore
EDGE_TARGET_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc=EDGE_TARGET_TEXT)  # type: ignore

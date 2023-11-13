# -*- coding: utf-8 -*-
from typing import ClassVar

from pydantic import Field, field_validator

from kiara.models import KiaraModel
from kiara.models.documentation import DocumentationMetadataModel
from kiara_plugin.network_analysis.defaults import (
    EDGE_COUNT_DUP_DIRECTED_TEXT,
    EDGE_COUNT_DUP_UNDIRECTED_TEXT,
    EDGE_IDX_DUP_DIRECTED_TEXT,
    EDGE_IDX_DUP_UNDIRECTED_TEXT,
    EDGE_SOURCE_TEXT,
    EDGE_TARGET_TEXT,
    NODE_COUNT_EDGES_MULTI_TEXT,
    NODE_COUNT_EDGES_TEXT,
    NODE_COUNT_IN_EDGES_MULTI_TEXT,
    NODE_COUNT_IN_EDGES_TEXT,
    NODE_COUNT_OUT_EDGES_MULTI_TEXT,
    NODE_COUNT_OUT_EDGES_TEXT,
    NODE_ID_TEXT,
    NODE_LABEL_TEXT,
)


class NetworkNodeAttributeMetadata(KiaraModel):

    _kiara_model_id: ClassVar = "metadata.network_node_attribute"

    doc: DocumentationMetadataModel = Field(
        description="Explanation what this attribute is about.",
        default_factory=DocumentationMetadataModel.create,
    )
    computed_attribute: bool = Field(
        description="Whether this is the default attribute that is always automatically added by kiara.",
        default=False,
    )

    @field_validator("doc", mode="before")
    @classmethod
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)


class NetworkEdgeAttributeMetadata(KiaraModel):

    _kiara_model_id: ClassVar = "metadata.network_edge_attribute"

    doc: DocumentationMetadataModel = Field(
        description="Explanation what this attribute is about.",
        default_factory=DocumentationMetadataModel.create,
    )
    computed_attribute: bool = Field(
        description="Whether this is the computed attribute that is automatically added by kiara.",
        default=False,
    )

    @field_validator("doc", mode="before")
    @classmethod
    def validate_doc(cls, value):
        return DocumentationMetadataModel.create(value)


NODE_ID_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_ID_TEXT, computed_attribute=True)  # type: ignore
NODE_LABEL_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_LABEL_TEXT, computed_attribute=True)  # type: ignore

NODE_COUNT_EDGES_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_COUNT_EDGES_TEXT, computed_attribute=True)  # type: ignore
NODE_COUND_EDGES_MULTI_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_COUNT_EDGES_MULTI_TEXT, computed_attribute=True)  # type: ignore
NODE_COUNT_IN_EDGES_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_COUNT_IN_EDGES_TEXT, computed_attribute=True)  # type: ignore
NODE_COUNT_IN_EDGES_MULTI_COLUMN_METADATA = NetworkNodeAttributeMetadata(doc=NODE_COUNT_IN_EDGES_MULTI_TEXT, computed_attribute=True)  # type: ignore
NODE_COUNT_OUT_EDGES_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc=NODE_COUNT_OUT_EDGES_TEXT, computed_attribute=True)  # type: ignore
NODE_COUNT_OUT_EDGES_MULTI_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc=NODE_COUNT_OUT_EDGES_MULTI_TEXT, computed_attribute=True)  # type: ignore

EDGE_ID_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc="The unique id for the edge.", computed_attribute=True)  # type: ignore
EDGE_SOURCE_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc=EDGE_SOURCE_TEXT, computed_attribute=True)  # type: ignore
EDGE_TARGET_COLUMN_METADATA = NetworkEdgeAttributeMetadata(doc=EDGE_TARGET_TEXT, computed_attribute=True)  # type: ignore

EDGE_COUNT_DUP_DIRECTED_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
    doc=EDGE_COUNT_DUP_DIRECTED_TEXT, computed_attribute=True
)
EDGE_IDX_DUP_DIRECTED_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
    doc=EDGE_IDX_DUP_DIRECTED_TEXT, computed_attribute=True
)
EDGE_COUNT_DUP_UNDIRECTED_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
    doc=EDGE_COUNT_DUP_UNDIRECTED_TEXT, computed_attribute=True
)
EDGE_IDX_DUP_UNDIRECTED_COLUMN_METADATA = NetworkEdgeAttributeMetadata(
    doc=EDGE_IDX_DUP_UNDIRECTED_TEXT, computed_attribute=True
)

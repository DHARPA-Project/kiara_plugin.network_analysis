# -*- coding: utf-8 -*-

"""This module contains the metadata models that are used in the ``kiara_modules.network_analysis`` package.

Metadata models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata that
is attached to data, as well as *kiara* modules. It is possible to register metadata using a JSON schema string, but
it is recommended to create a metadata model, because it is much easier overall.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel].
"""
from pydantic import BaseModel, Field


class GraphMetadata(BaseModel):

    number_of_nodes: int = Field(description="The number of nodes in this graph.")
    number_of_edges: int = Field(description="The number of edges in this graph.")
    directed: bool = Field(description="Whether the graph is directed or not.")
    density: float = Field(description="The density of the graph.")

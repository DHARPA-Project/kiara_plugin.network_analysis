# -*- coding: utf-8 -*-
from typing import Any, ClassVar, Dict, Union

from pydantic import Field, model_validator

from kiara.models import KiaraModel
from kiara_plugin.core_types.defaults import DEFAULT_MODEL_KEY
from kiara_plugin.network_analysis.defaults import AGGREGATION_FUNCTION_NAME


class AttributeMapStrategy(KiaraModel):

    _kiara_model_id: ClassVar = "input.attribute_map_strategy"

    @model_validator(mode="before")
    @classmethod
    def pre_validate_model(cls, values: Dict[str, Any]):

        if len(values) == 1 and DEFAULT_MODEL_KEY in values.keys():

            token = values[DEFAULT_MODEL_KEY]
            source_column_name = None
            if "=" in token:
                target_column_name, func_token = token.split("=", maxsplit=1)
                if "(" in func_token:

                    func, source_column_name = func_token.split("(", maxsplit=1)
                    source_column_name = source_column_name.strip()
                    func = func.strip()
                    if not source_column_name.endswith(")"):
                        raise ValueError(
                            f"Invalid function definition, missing closing parenthesis: {func_token}"
                        )

                    source_column_name = source_column_name[:-1]
                else:
                    func = func_token.strip()

            else:
                target_column_name = token
                func = None

            if not source_column_name:
                source_column_name = target_column_name
            result = {
                "target_column_name": target_column_name,
                "source_column_name": source_column_name,
                "transform_function": func.lower() if func else None,
            }

            return result

        if not values.get("target_column_name", None):
            raise ValueError("No 'target_column_name' specified.")
        if not values.get("source_column_name", None):
            values["source_column_name"] = values["target_column_name"]

        values["transform_function"] = (
            values["transform_function"].lower()
            if values["transform_function"]
            else None
        )
        return values

    target_column_name: str = Field(
        description="The name of the attribute in the resulting network_data instance."
    )
    source_column_name: str = Field(
        description="The name of the attribute (or attributes) in the source network_data instance. Defaults to 'target_column_name'."
    )
    transform_function: Union[AGGREGATION_FUNCTION_NAME, None] = Field(  # type: ignore
        description="The name of the function to apply to the attribute(s).",
        default=None,
    )

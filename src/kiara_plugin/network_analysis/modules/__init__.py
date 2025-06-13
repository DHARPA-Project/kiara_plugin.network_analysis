# -*- coding: utf-8 -*-

from pydantic import Field

from kiara.api import KiaraModule, KiaraModuleConfig, ValueMap, ValueMapSchema


class ExampleModuleConfig(KiaraModuleConfig):
    separator: str = Field(
        description="The seperator between the two strings.", default=" - "
    )


class ExampleModule(KiaraModule):
    """A very simple example module; concatenate two strings.

    The purpose of this modules is to show the main elements of a [`KiaraModule`][kiara.modules.KiaraModule]:

    - ***the (optional) configuration class***: must inherit from [`KiaraModuleConfig`][kiara.modules.KiaraModuleConfig], and the config class must be set as the `_config_cls` attribute
         on the `KiaraModule` class. Configuration values can be retrieved via the [`self.get_config_value(key)`][kiara.modules.KiaraModule.get_config_value] method
    - ***the inputs description***: must return a dictionary, containing the input name(s) as keys, and another dictionary containing type_name information
         and documentation about the input data as value
    - ***the outputs description***: must return a dictionary, containing the output name(s) as keys, and another dictionary containing type_name information
         and documentation about the output data as value
    - ***the ``process`` method***: this is where the actual work gets done. Input data can be accessed via ``inputs.get_value_data(key)``, results
         can be set with the ``outputs.set_value(key, value)`` method

    Example:

        This example module can be tested on the commandline with the ``kiara run`` command:

        ```
        kiara run core_types.example text_1="xxx" text_2="yyy"
        ```
    """

    _config_cls = ExampleModuleConfig
    _module_type_name = "network_analysis.example"

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        inputs = {
            "text_1": {"type": "string", "doc": "The first text."},
            "text_2": {"type": "string", "doc": "The second text."},
        }

        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:
        outputs = {
            "text": {
                "type": "string",
                "doc": "The concatenated text.",
            }
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:
        separator = self.get_config_value("separator")

        text_1 = inputs.get_value_data("text_1")
        text_2 = inputs.get_value_data("text_2")

        result = text_1 + separator + text_2
        outputs.set_value("text", result)

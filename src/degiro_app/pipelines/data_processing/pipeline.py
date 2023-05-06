"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import type_converter


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=type_converter,
                inputs="degiro_app-acount-raw",
                outputs="degiro_app-account-type_check",
                name="type_converted_node",
            )
        ]
    )

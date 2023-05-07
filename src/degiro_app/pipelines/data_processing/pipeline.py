"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import type_converter, split_description


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_description,
                inputs="degiro_app-account-raw",
                outputs="degiro_app-account-desc_split",
                name="split_description_node",
            ),
        ]
    )

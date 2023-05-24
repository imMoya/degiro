"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    node_dataset_clean,
    node_return_portfolio,
    return_dividends,
    return_portfolio,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=node_dataset_clean,
                inputs="degiro_app-account-raw-path",
                outputs="degiro_app-account-clean",
                name="split_description_node",
            ),
            node(
                func=return_portfolio,
                inputs=["degiro_app-account-clean"],
                outputs="portfolio_summary",
                name="return_on_portfolio_node",
            ),
            node(
                func=return_dividends,
                inputs=["degiro_app-account-clean"],
                outputs="dividends_summary",
                name="dividends_node",
            ),
        ]
    )

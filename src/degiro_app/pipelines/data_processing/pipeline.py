"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.8
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    split_description,
    return_on_stock_complete,
    return_portfolio,
    return_dividends,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=split_description,
                inputs="degiro_app-account-raw",
                outputs="degiro_app-account-clean",
                name="split_description_node",
            ),
            node(
                func=return_on_stock_complete,
                inputs=["degiro_app-account-clean", "stock"],
                outputs="stock_summary",
                name="return_on_stock_node",
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

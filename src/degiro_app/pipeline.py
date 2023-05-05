"""Class to postprocess and clean dataset"""
import pandas as pd

from degiro_app.dataset import DatasetProcessor


class Pipeline:
    def __init__(self, path):
        self.path = path
        self.pipeline = []

    def add_step(self, method, *args, **kwargs):
        self.pipeline.append((method, args, kwargs))

    def run_pipeline(self, out_path):
        processor = DatasetProcessor(self.path)
        for step in self.pipeline:
            method, args, kwargs = step
            method(processor, *args, **kwargs)
        processor.data.to_csv(out_path, index=False)
        return processor.data

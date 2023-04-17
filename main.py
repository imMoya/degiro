from src import DatasetProcessor, Pipeline

if __name__ == "__main__":
    path = "data/Account.csv"
    
    pipeline = Pipeline(path)
    pipeline.add_step(DatasetProcessor.replace_commas, "Unnamed: 8")
    pipeline.add_step(DatasetProcessor.rename_col, 'Producto', 'Name')
    pipeline.run_pipeline(out_path="data/data.csv")


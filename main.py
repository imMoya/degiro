from src import DatasetProcessor, Pipeline

if __name__ == "__main__":
    path = "data/Account.csv"
    
    pipeline = Pipeline(path)
    # Cleaning pipeline
    pipeline.add_step(DatasetProcessor.replace_commas, "Unnamed: 8")
    pipeline.add_step(DatasetProcessor.replace_commas, "Unnamed: 10")
    pipeline.add_step(DatasetProcessor.rename_col, 'Producto', 'Name')
    pipeline.add_step(DatasetProcessor.drop_col, 'ID Orden')
    pipeline.add_step(DatasetProcessor.drop_col, 'Fecha valor')
    pipeline.add_step(DatasetProcessor.drop_na, 'Descripción')
    pipeline.add_step(DatasetProcessor.apply_split_string_to_col, 'Descripción')
    pipeline.add_step(DatasetProcessor.replace_string_in_col, 'Action', 'Compra', 'buy')
    pipeline.add_step(DatasetProcessor.replace_string_in_col, 'Action', 'Venta', 'sell')
    pipeline.add_step(DatasetProcessor.date_and_hour_transform, 'Fecha', 'Hora')
    pipeline.add_step(DatasetProcessor.convert_to_float, 'Unnamed: 8')
    pipeline.add_step(DatasetProcessor.convert_to_float, 'Unnamed: 10')
    pipeline.add_step(DatasetProcessor.convert_to_float, 'Price')
    pipeline.add_step(DatasetProcessor.convert_to_float, 'Number')
    pipeline.run_pipeline(out_path="data/data.csv")
    # Computing taxes


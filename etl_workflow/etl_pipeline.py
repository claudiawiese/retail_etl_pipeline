from extract import SQLiteExtractor
from load import SQLiteLoader
import config

class ETLPipeline(SQLiteExtractor, SQLiteLoader):
    pass  # Inherits all methods from the components

if __name__ == "__main__":
    etl_pipeline = ETLPipeline(config.CONFIG)
    etl_pipeline.run()
import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from schema import get_acquirer_schema , get_gateway_schema ,get_combined_schema

class FileProcessor:
    def __init__(self, spark):
        self.spark = spark
        self.hudi_path = "/home/shraddha/hudifile"  
        self.acquirer_schema = get_acquirer_schema()
        self.gateway_schema= get_gateway_schema()
        self.combined_schema = get_combined_schema()  

      #read file 
    def read_csv_file(self, file_path):
        
        try:
            return self.spark.read.schema(self.gateway_schema).option("header", "false").csv(file_path)
        except Exception as e:
            print(f"Error reading CSV file {file_path}: {e}")
            raise

    def read_excel_file(self, file_path):
        
        try:
            return self.spark.read.format("com.crealytics.spark.excel") \
                       .schema(self.acquirer_schema) \
                       .option("dataAddress", "'Sheet1'!A2") \
                       .option("header", "false") \
                       .load(file_path)

        except Exception as e:
            print(f"Error reading Excel file {file_path}: {e}") 
            raise 
    
    def combined_columns(self, df, schema):
        """Select columns from the DataFrame according to the combined schema, filling missing with nulls."""
        for field in schema.fields:
            if field.name not in df.columns:
            # Add the missing column with null values and cast to the appropriate data type
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        
        return df.select([field.name for field in schema.fields])
    def rename_columns(self, df):
        """Rename columns based on their prefixes (acquirer_ -> acq_, gateway_ -> gate_)."""
        new_columns = []

        for column in df.columns:
            if column.startswith("acquirer_"):
                new_columns.append(F.col(column).alias(column.replace("acquirer_", "acq_")))
            elif column.startswith("gateway_"):
                new_columns.append(F.col(column).alias(column.replace("gateway_", "gate_")))
            else:
                new_columns.append(F.col(column).alias(column))
        return df.select(*new_columns) 
    # write data into hudi table 
    def write_to_hudi(self, df: DataFrame):
        """Write DataFrame to Hudi table, either inserting or updating."""
        hudi_options = {
            "hoodie.table.name": "huditable",
            "hoodie.datasource.write.recordkey.field": "record_key",
            "hoodie.datasource.write.precombine.field": "ts",
            "hoodie.datasource.write.operation": "upsert"
        } 

        
        df.write.format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save(self.hudi_path)
        print("Data successfully written to Hudi table.")
        
    def read_from_hudi(self):
    
        try:
            return self.spark.read.format("hudi").load(self.hudi_path)
        except Exception as e:
            print(f"Error reading from Hudi: {e}")
            return None

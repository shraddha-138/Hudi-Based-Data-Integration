from fileprocessor import FileProcessor
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

class AcquirerProcessor(FileProcessor):
    

    def process_acquirer_file(self, file_path):
        """Process the acquirer file and write data to Hudi based on join conditions."""
        
        acquirer_df = self.read_excel_file(file_path)

    

        acquirer_df = self._process_acquirer_data(acquirer_df)
        #  Process acquirer data
        acquirer_df = self.combined_columns(acquirer_df, self.combined_schema)
        print("Acquirer DataFrame Schema:")
        acquirer_df.printSchema()
        print(f"Row count: {acquirer_df.count()}")
        # Read existing records from Hudi table if it exists
        existing_df = self.read_from_hudi()

        if existing_df is None:
            print("No existing Hudi table. Writing acquirer data to create table.")
            self.write_to_hudi(acquirer_df)
            
        else:
            # Perform join and upsert logic if table already exists
            join_df = self.join_acquirer_with_gateway(acquirer_df, existing_df)
            join_df.printSchema()
            self.write_to_hudi(join_df)
        written_df = self.read_from_hudi()
        if written_df:
            written_df.show(10, truncate=False)  
        else:
            print("No data found in Hudi table.")
    
    def join_acquirer_with_gateway(self, acquirer_df, existing_df):
        """Join acquirer DataFrame with existing gateway DataFrame and handle duplicate columns."""
    # Add prefixes to columns to distinguish between acquirer and gateway DataFrames
        acquirer_rename = self.rename_columns(acquirer_df)
        

    # Perform the join
        join_df = acquirer_rename.join(
            existing_df,
            (acquirer_rename["acq_retrieval_reference_number"] == existing_df["gateway_retrieval_reference_number"]) &
            (acquirer_rename["acq_transaction_type"] == existing_df["gateway_transaction_message_type"]),
            "left"
        )

    # Resolve duplicate columns: 
        upsert_df = join_df.select(
                F.coalesce(join_df.acq_submitted_date, join_df.acquirer_submitted_date).alias("acquirer_submitted_date"),
                F.coalesce(join_df.acq_merchant_id, join_df.acquirer_merchant_id).alias("acquirer_merchant_id"),
                F.coalesce(join_df.acq_merchant_name, join_df.acquirer_merchant_name).alias("acquirer_merchant_name"),
                F.coalesce(join_df.acq_transaction_type, join_df.acquirer_transaction_type).alias("acquirer_transaction_type"),
                F.coalesce(join_df.acq_transaction_status, join_df.acquirer_transaction_status).alias("acquirer_transaction_status"),
                F.coalesce(join_df.acq_card_type, join_df.acquirer_card_type).alias("acquirer_card_type"),
                F.coalesce(join_df.acq_card_number, join_df.acquirer_card_number).alias("acquirer_card_number"),
                F.coalesce(join_df.acq_submitted_currency, join_df.acquirer_submitted_currency).alias("acquirer_submitted_currency"),
                F.coalesce(join_df.acq_submitted_amount, join_df.acquirer_submitted_amount).alias("acquirer_submitted_amount"),
                F.coalesce(join_df.acq_retrieval_reference_number, join_df.acquirer_retrieval_reference_number).alias("acquirer_retrieval_reference_number"),
                F.coalesce(join_df.acq_reject_code, join_df.acquirer_reject_code).alias("acquirer_reject_code"),
                F.coalesce(join_df.acq_actual_fee_program_indicator, join_df.acquirer_actual_fee_program_indicator).alias("acquirer_actual_fee_program_indicator"),
                F.coalesce(join_df.acq_interchange_differential_currency, join_df.acquirer_interchange_differential_currency).alias("acquirer_interchange_differential_currency"),
                F.coalesce(join_df.acq_interchange_differential, join_df.acquirer_interchange_differential).alias("acquirer_interchange_differential"),
                F.coalesce(join_df.acq_fx_gain_loss_currency, join_df.acquirer_fx_gain_loss_currency).alias("acquirer_fx_gain_loss_currency"),
                F.coalesce(join_df.acq_fx_gain_loss, join_df.acquirer_fx_gain_loss).alias("acquirer_fx_gain_loss"),
                F.coalesce(join_df.acq_planned_interchange_fee_currency, join_df.acquirer_planned_interchange_fee_currency).alias("acquirer_planned_interchange_fee_currency"),
                F.coalesce(join_df.acq_planned_interchange_fee, join_df.acquirer_planned_interchange_fee).alias("acquirer_planned_interchange_fee"),
                F.coalesce(join_df.acq_actual_interchange_fee_currency, join_df.acquirer_actual_interchange_fee_currency).alias("acquirer_actual_interchange_fee_currency"),
                F.coalesce(join_df.acq_actual_interchange_fee, join_df.acquirer_actual_interchange_fee).alias("acquirer_actual_interchange_fee"),
                F.coalesce(join_df.acq_discount_charged_currency, join_df.acquirer_discount_charged_currency).alias("acquirer_discount_charged_currency"),
                F.coalesce(join_df.acq_discount_charged, join_df.acquirer_discount_charged).alias("acquirer_discount_charged"),
                F.coalesce(join_df.acq_ach_indicator, join_df.acquirer_ach_indicator).alias("acquirer_ach_indicator"),
                F.coalesce(join_df.acq_auth_code, join_df.acquirer_auth_code).alias("acquirer_auth_code"),
                F.coalesce(join_df.acq_pos_entry, join_df.acquirer_pos_entry).alias("acquirer_pos_entry"),
                F.coalesce(join_df.acq_transaction_date, join_df.acquirer_transaction_date).alias("acquirer_transaction_date"),
                F.coalesce(join_df.acq_deposit_date, join_df.acquirer_deposit_date).alias("acquirer_deposit_date"),
                F.coalesce(join_df.acq_batch_amount_currency, join_df.acquirer_batch_amount_currency).alias("acquirer_batch_amount_currency"),
                F.coalesce(join_df.acq_batch_amount, join_df.acquirer_batch_amount).alias("acquirer_batch_amount"),
                F.coalesce(join_df.acq_batch_id, join_df.acquirer_batch_id).alias("acquirer_batch_id"),
                F.coalesce(join_df.acq_terminal_id, join_df.acquirer_terminal_id).alias("acquirer_terminal_id"),
                F.coalesce(join_df.acq_acq_reference_number, join_df.acquirer_acquirer_reference_number).alias("acquirer_acquirer_reference_number"),
                F.coalesce(join_df.acq_transaction_currency_code, join_df.acquirer_transaction_currency_code).alias("acquirer_transaction_currency_code"),
                F.coalesce(join_df.acq_conversion_rate, join_df.acquirer_conversion_rate).alias("acquirer_conversion_rate"),
                F.coalesce(join_df.acq_mark_up_percentage, join_df.acquirer_mark_up_percentage).alias("acquirer_mark_up_percentage"),
                F.coalesce(join_df.acq_cardholder_currency, join_df.acquirer_cardholder_currency).alias("acquirer_cardholder_currency"),
                F.coalesce(join_df.acq_cardholder_amount, join_df.acquirer_cardholder_amount).alias("acquirer_cardholder_amount"),
                F.coalesce(join_df.acq_settled_currency, join_df.acquirer_settled_currency).alias("acquirer_settled_currency"),
                F.coalesce(join_df.acq_settled_amount, join_df.acquirer_settled_amount).alias("acquirer_settled_amount"),
                F.coalesce(join_df.acq_total_transaction_revenue_currency, join_df.acquirer_total_transaction_revenue_currency).alias("acquirer_total_transaction_revenue_currency"),
                F.coalesce(join_df.acq_total_transaction_revenue, join_df.acquirer_total_transaction_revenue).alias("acquirer_total_transaction_revenue"),
                F.coalesce(join_df.acq_total_acq_revenue_currency, join_df.acquirer_total_acquirer_revenue_currency).alias("acquirer_total_acquirer_revenue_currency"),
                F.coalesce(join_df.acq_total_acq_revenue, join_df.acquirer_total_acquirer_revenue).alias("acquirer_total_acquirer_revenue"),
                F.coalesce(join_df.acq_total_planet_revenue_currency, join_df.acquirer_total_planet_revenue_currency).alias("acquirer_total_planet_revenue_currency"),
                F.coalesce(join_df.acq_total_planet_revenue, join_df.acquirer_total_planet_revenue).alias("acquirer_total_planet_revenue"),
                F.coalesce(join_df.acq_total_merchant_revenue_currency, join_df.acquirer_total_merchant_revenue_currency).alias("acquirer_total_merchant_revenue_currency"),
                F.coalesce(join_df.acq_total_merchant_revenue, join_df.acquirer_total_merchant_revenue).alias("acquirer_total_merchant_revenue"),
                F.coalesce(join_df.acq_total_other_participant_revenue_currency, join_df.acquirer_total_other_participant_revenue_currency).alias("acquirer_total_other_participant_revenue_currency"),
                F.coalesce(join_df.acq_total_other_participant_revenue, join_df.acquirer_total_other_participant_revenue).alias("acquirer_total_other_participant_revenue"),
                F.coalesce(join_df.acq_ecom_indicator, join_df.acquirer_ecom_indicator).alias("acquirer_ecom_indicator"),
                F.coalesce(join_df.acq_moto_indicator, join_df.acquirer_moto_indicator).alias("acquirer_moto_indicator"),
                F.coalesce(join_df.acq_issuer_country, join_df.acquirer_issuer_country).alias("acquirer_issuer_country"),
                F.coalesce(join_df.acq_card_cd_type, join_df.acquirer_card_cd_type).alias("acquirer_card_cd_type"),
                F.coalesce(join_df.gate_record_type, join_df.gateway_record_type).alias("gateway_record_type"),
                F.coalesce(join_df.gate_record_sequence, join_df.gateway_record_sequence).alias("gateway_record_sequence"),
                F.coalesce(join_df.gate_transaction_message_type, join_df.gateway_transaction_message_type).alias("gateway_transaction_message_type"),
                F.coalesce(join_df.gate_card_number, join_df.gateway_card_number).alias("gateway_card_number"),
                F.coalesce(join_df.gate_transaction_amount, join_df.gateway_transaction_amount).alias("gateway_transaction_amount"),
                F.coalesce(join_df.gate_system_trace_no, join_df.gateway_system_trace_no).alias("gateway_system_trace_no"),
                F.coalesce(join_df.gate_local_time, join_df.gateway_local_time).alias("gateway_local_time"),
                F.coalesce(join_df.gate_local_date, join_df.gateway_local_date).alias("gateway_local_date"),
                F.coalesce(join_df.gate_card_expiry_date, join_df.gateway_card_expiry_date).alias("gateway_card_expiry_date"),
                F.coalesce(join_df.gate_pos_entry_mode, join_df.gateway_pos_entry_mode).alias("gateway_pos_entry_mode"),
                F.coalesce(join_df.gate_retrieval_reference_number, join_df.gateway_retrieval_reference_number).alias("gateway_retrieval_reference_number"),
                F.coalesce(join_df.gate_authorization_code, join_df.gateway_authorization_code).alias("gateway_authorization_code"),
                F.coalesce(join_df.gate_response_code, join_df.gateway_response_code).alias("gateway_response_code"),
                F.coalesce(join_df.gate_terminal_id, join_df.gateway_terminal_id).alias("gateway_terminal_id"),
                F.coalesce(join_df.gate_merchant_id, join_df.gateway_merchant_id).alias("gateway_merchant_id"),
                F.coalesce(join_df.gate_acquirer_id, join_df.gateway_acquirer_id).alias("gateway_acquirer_id"),
                F.coalesce(join_df.gate_dcc_indicator, join_df.gateway_dcc_indicator).alias("gateway_dcc_indicator"),
                F.coalesce(join_df.gate_dcc_amount, join_df.gateway_dcc_amount).alias("gateway_dcc_amount"),
                F.coalesce(join_df.gate_dcc_currency, join_df.gateway_dcc_currency).alias("gateway_dcc_currency"),
                F.coalesce(join_df.gate_dcc_rate, join_df.gateway_dcc_rate).alias("gateway_dcc_rate"),
               
                F.coalesce(existing_df["record_key"], F.expr("uuid()")).alias("record_key"),  # Use expr to generate a UUID
                F.current_timestamp().alias("ts") 
               
            
                
        )
        upsert_df = upsert_df.withColumn(
                 "record_key",
                 F.when(F.col("record_key").isNotNull(), F.col("record_key"))  # If record_key exists, use it
                  .otherwise(F.expr("uuid()"))  # Else generate a new UUID
        )
        
        return upsert_df 

    
       
    def _process_acquirer_data(self, df):
        """Process acquirer data to ensure required transformations and columns are present."""
        map_transaction_type = {
            "Sale": "Sale",
            "Credit/Return": "Refund",
            "Sale Chargeback": "Chargeback",
            "Sale Representment": "Representment"
        }

        # Register the UDF for mapping transaction codes
        map_transaction_code_udf = F.udf(lambda code: map_transaction_type.get(code, code), StringType())
        df = df.withColumn("acquirer_transaction_type", map_transaction_code_udf(F.col("acquirer_transaction_type")))

        df = df.withColumn("record_key", (F.expr("uuid()")))
     # Create a unique record key
        df = df.withColumn("ts", F.current_timestamp())  # Timestamp for precombine
        return df
    
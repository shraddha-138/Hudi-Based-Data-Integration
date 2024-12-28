from fileprocessor import FileProcessor
from pyspark.sql import functions as F
from pyspark.sql.types import StringType 

class GatewayProcessor(FileProcessor):
    
    def process_gateway_file(self, file_path):
        """Process the gateway file and write data to Hudi based on join conditions."""
        
        gateway_df = self.read_csv_file(file_path)
        gateway_df.printSchema()

        gateway_df = self._process_gateway_data(gateway_df)
        gateway_df = self.combined_columns(gateway_df, self.combined_schema)
    

        
    

    # Read existing records from Hudi table if it exists
        existing_df = self.read_from_hudi()

        if existing_df is None:
            print("No existing Hudi table. Writing gateway data to create table.")
            self.write_to_hudi(gateway_df)
        else:
            # Perform join and upsert logic if table already exists
            joined_df = self.join_gateway_with_acquirer(gateway_df, existing_df)
            joined_df.show()
            self.write_to_hudi(joined_df)

        
        self.read_from_hudi()

    

    def join_gateway_with_acquirer(self, gateway_df, existing_df):
    
    # Rename columns for clarity during join
        gateway_rename = self.rename_columns(gateway_df)

    # Perform the join
        join_df = gateway_rename.join(
            existing_df,
            (gateway_rename["gate_retrieval_reference_number"] == existing_df["acquirer_retrieval_reference_number"]) &
            (gateway_rename["gate_transaction_message_type"] == existing_df["acquirer_transaction_type"]),
            "left"
        )


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
    

    def _process_gateway_data(self, df):
        df = df.filter((F.col("gateway_record_type") != 'H') & (F.col("gateway_record_type") != 'T'))
        map_transaction_type = {
           "01":"Authorization",
           "03":"Sale",
           "04":"Refund",
           "05":"Reversal"
        }
        def map_transaction_code(code):
            return map_transaction_type.get(code, code)
        map_transaction_code_udf = F.udf(map_transaction_code, F.StringType())


        df = df.withColumn("gateway_transaction_message_type", map_transaction_code_udf(F.col("gateway_transaction_message_type")))

        df = df.withColumn("record_key", (F.expr("uuid()")))
        df = df.withColumn("ts", F.current_timestamp())
        return df  
    


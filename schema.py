from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def get_gateway_schema():
     return StructType([
            StructField("gateway_record_type", StringType(), True),
            StructField("gateway_record_sequence", StringType(), True),
            StructField("gateway_transaction_message_type", StringType(), True),
            StructField("gateway_card_number", StringType(), True),
            StructField("gateway_transaction_amount", StringType(), True),
            StructField("gateway_system_trace_no", StringType(), True),
            StructField("gateway_local_time", StringType(), True),
            StructField("gateway_local_date", StringType(), True),
            StructField("gateway_card_expiry_date", StringType(), True),
            StructField("gateway_pos_entry_mode", StringType(), True),
            StructField("gateway_retrieval_reference_number", StringType(), True),
            StructField("gateway_authorization_code", StringType(), True),
            StructField("gateway_response_code", StringType(), True),
            StructField("gateway_terminal_id", StringType(), True),
            StructField("gateway_merchant_id", StringType(), True),
            StructField("gateway_acquirer_id", StringType(), True),
            StructField("gateway_dcc_indicator", StringType(), True),
            StructField("gateway_dcc_amount", StringType(), True),
            StructField("gateway_dcc_currency", StringType(), True),
            StructField("gateway_dcc_rate", StringType(), True)
      ])     
def get_acquirer_schema():
    return StructType([
            StructField("acquirer_submitted_date", StringType(), True),
            StructField("acquirer_merchant_id", StringType(), True),
            StructField("acquirer_merchant_name", StringType(), True),
            StructField("acquirer_transaction_type", StringType(), True),
            StructField("acquirer_transaction_status", StringType(), True),
            StructField("acquirer_card_type", StringType(), True),
            StructField("acquirer_card_number", StringType(), True),
            StructField("acquirer_submitted_currency", StringType(), True),
            StructField("acquirer_submitted_amount", StringType(), True),
            StructField("acquirer_retrieval_reference_number", StringType(), True),
            StructField("acquirer_reject_code", StringType(), True),
            StructField("acquirer_actual_fee_program_indicator", StringType(), True),
            StructField("acquirer_interchange_differential_currency", StringType(), True),
            StructField("acquirer_interchange_differential", StringType(), True),
            StructField("acquirer_fx_gain_loss_currency", StringType(), True),
            StructField("acquirer_fx_gain_loss", StringType(), True),
            StructField("acquirer_planned_interchange_fee_currency", StringType(), True),
            StructField("acquirer_planned_interchange_fee", StringType(), True),
            StructField("acquirer_actual_interchange_fee_currency", StringType(), True),
            StructField("acquirer_actual_interchange_fee", StringType(), True),
            StructField("acquirer_discount_charged_currency", StringType(), True),
            StructField("acquirer_discount_charged", StringType(), True),
            StructField("acquirer_ach_indicator", StringType(), True),
            StructField("acquirer_auth_code", StringType(), True),
            StructField("acquirer_pos_entry", StringType(), True),
            StructField("acquirer_transaction_date", StringType(), True),
            StructField("acquirer_deposit_date", StringType(), True),
            StructField("acquirer_batch_amount_currency", StringType(), True),
            StructField("acquirer_batch_amount", StringType(), True),
            StructField("acquirer_batch_id", StringType(), True),
            StructField("acquirer_terminal_id", StringType(), True),
            StructField("acquirer_acquirer_reference_number", StringType(), True),
            StructField("acquirer_transaction_currency_code", StringType(), True),
            StructField("acquirer_conversion_rate", StringType(), True),
            StructField("acquirer_mark_up_percentage", StringType(), True),
            StructField("acquirer_cardholder_currency", StringType(), True),
            StructField("acquirer_cardholder_amount", StringType(), True),
            StructField("acquirer_settled_currency", StringType(), True),
            StructField("acquirer_settled_amount", StringType(), True),
            StructField("acquirer_total_transaction_revenue_currency", StringType(), True),
            StructField("acquirer_total_transaction_revenue", StringType(), True),
            StructField("acquirer_total_acquirer_revenue_currency", StringType(), True),
            StructField("acquirer_total_acquirer_revenue", StringType(), True),
            StructField("acquirer_total_planet_revenue_currency", StringType(), True),
            StructField("acquirer_total_planet_revenue", StringType(), True),
            StructField("acquirer_total_merchant_revenue_currency", StringType(), True),
            StructField("acquirer_total_merchant_revenue", StringType(), True),
            StructField("acquirer_total_other_participant_revenue_currency", StringType(), True),
            StructField("acquirer_total_other_participant_revenue", StringType(), True),
            StructField("acquirer_ecom_indicator", StringType(), True),
            StructField("acquirer_moto_indicator", StringType(), True),
            StructField("acquirer_issuer_country", StringType(), True),
            StructField("acquirer_card_cd_type", StringType(), True)
      ]) 
def get_combined_schema():
    
        gateway_schema = get_gateway_schema()
        acquirer_schema = get_acquirer_schema()

        combined_schema = gateway_schema.add("ts", TimestampType(), True)
        combined_schema = combined_schema.add("record_key", StringType(), True)
        for field in acquirer_schema.fields:
            combined_schema = combined_schema.add(field.name, field.dataType, True)
    
        return combined_schema 
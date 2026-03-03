from pyspark.sql import types as T

final_schema = T.StructType([
    T.StructField('client_id', T.StringType(), True),
    T.StructField('document_number', T.StringType(), True),
    T.StructField('document_type', T.StringType(), True),
    T.StructField('facebook_id', T.StringType(), True),
    T.StructField('gift_code', T.StringType(), True),
    T.StructField('merchant_id', T.StringType(), True),
    T.StructField('phone_number', T.StringType(), True),
    T.StructField('reference1', T.StringType(), True),
    T.StructField('sub_type', T.StringType(), True),
    T.StructField('subsidiary_id', T.StringType(), True),
    T.StructField('transaction_id', T.StringType(), True),
    T.StructField('tstamp', T.TimestampType(), True),
    T.StructField('value', T.DecimalType(), True),
    T.StructField('id_reward', T.StringType(), False),
])

from pyspark.sql import types as T


final_schema = T.StructType([
    T.StructField('creation_date', T.DateType(), True),
    T.StructField('customer_id', T.StringType(), True),
    T.StructField('email', T.StringType(), True),
    T.StructField('first_name', T.StringType(), True),
    T.StructField('notification_date', T.DateType(), True),
    T.StructField('notified', T.StringType(), True),
    T.StructField('owner', T.StringType(), True),
    T.StructField('phone_number', T.StringType(), True),
    T.StructField('products_credits', T.StringType(), True),
    T.StructField('products_paypal', T.StringType(), True),
    T.StructField('products_remittances', T.StringType(), True),
    T.StructField('products_savings_account', T.StringType(), True),
    T.StructField('risk', T.StringType(), True),
    T.StructField('update_date', T.DateType(), True),
    T.StructField('notifications', T.ArrayType(
        T.StructType([
            T.StructField('message_id', T.StringType(), True),
            T.StructField('notification_date', T.StringType(), True),
            T.StructField('notified', T.StringType(), True),
            T.StructField('notified_date', T.StringType(), True),
            T.StructField('type', T.StringType(), True),
        ])
    , False), True),
    T.StructField('id_reward', T.StringType(), True),
    T.StructField('flag_event_status', T.StringType(), False), # Only if you have active event_status
])

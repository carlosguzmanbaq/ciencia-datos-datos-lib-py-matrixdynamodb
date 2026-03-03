from pyspark.sql import types as T


schema_full = T.StructType([
    T.StructField('Item', T.StructType([
        T.StructField('creationDate', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('customerId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('email', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('firstName', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('notificationDate', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('notifications', T.StructType([
            T.StructField('L', T.ArrayType(
                T.StructType([
                    T.StructField('M', T.StructType([
                        T.StructField('messageId', T.StructType([
                            T.StructField('NULL', T.BooleanType(), True),
                            T.StructField('S', T.StringType(), True),
                        ]), True),
                        T.StructField('notificationDate', T.StructType([
                            T.StructField('S', T.StringType(), True),
                        ]), True),
                        T.StructField('notified', T.StructType([
                            T.StructField('BOOL', T.BooleanType(), True),
                        ]), True),
                        T.StructField('notifiedDate', T.StructType([
                            T.StructField('NULL', T.BooleanType(), True),
                            T.StructField('S', T.StringType(), True),
                        ]), True),
                        T.StructField('type', T.StructType([
                            T.StructField('S', T.StringType(), True),
                        ]), True),
                    ]), True),
                ])
            , True), True),
        ]), True),
        T.StructField('notified', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('owner', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('phoneNumber', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('products', T.StructType([
            T.StructField('M', T.StructType([
                T.StructField('credits', T.StructType([
                    T.StructField('BOOL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('paypal', T.StructType([
                    T.StructField('BOOL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('remittances', T.StructType([
                    T.StructField('BOOL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('savingsAccount', T.StructType([
                    T.StructField('BOOL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
            ]), True),
        ]), True),
        T.StructField('risk', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('updateDate', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
    ]), True),
])
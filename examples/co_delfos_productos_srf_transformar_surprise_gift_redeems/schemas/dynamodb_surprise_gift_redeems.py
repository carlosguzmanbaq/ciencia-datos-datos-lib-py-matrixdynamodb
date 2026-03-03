from pyspark.sql import types as T

schema_full = T.StructType([
    T.StructField('Item', T.StructType([
        T.StructField('clientId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('documentNumber', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('documentType', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('facebookId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('giftCode', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('merchantId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('phoneNumber', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('reference1', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('subType', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('subsidiaryId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('transactionId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('tstamp', T.StructType([
            T.StructField('N', T.StringType(), True),
        ]), True),
        T.StructField('value', T.StructType([
            T.StructField('N', T.StringType(), True),
            T.StructField('S', T.StringType(), True),
        ]), True),
    ]), True),
])
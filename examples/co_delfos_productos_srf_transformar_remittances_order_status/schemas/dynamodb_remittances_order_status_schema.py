from pyspark.sql import types as T


schema_full = T.StructType([
    T.StructField('Item', T.StructType([
        T.StructField('conciliationId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('countRetry', T.StructType([
            T.StructField('N', T.StringType(), True),
        ]), True),
        T.StructField('creationDate', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('creationDateTStamp', T.StructType([
            T.StructField('N', T.StringType(), True),
        ]), True),
        T.StructField('customer', T.StructType([
            T.StructField('M', T.StructType([
                T.StructField('apellido1', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('apellido2', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('celular', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('cifId', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('correoElectronico', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('documentId', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('estadoId', T.StructType([
                    T.StructField('N', T.StringType(), True),
                    T.StructField('NULL', T.BooleanType(), True),
                ]), True),
                T.StructField('fechaExpedicion', T.StructType([
                    T.StructField('N', T.StringType(), True),
                    T.StructField('NULL', T.BooleanType(), True),
                ]), True),
                T.StructField('fechaNacimiento', T.StructType([
                    T.StructField('N', T.StringType(), True),
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('isFullSignUp', T.StructType([
                    T.StructField('BOOL', T.BooleanType(), True),
                ]), True),
                T.StructField('isUseRegulation', T.StructType([
                    T.StructField('BOOL', T.BooleanType(), True),
                ]), True),
                T.StructField('nombre1', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('nombre2', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('numeroCuenta', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('numeroId', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('tipoId', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('userCreation', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
            ]), True),
        ]), True),
        T.StructField('messageId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('order', T.StructType([
            T.StructField('M', T.StructType([
                T.StructField('beneDocumentID', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneDocumentType', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneFirstName', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneLastName', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneLastName2', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneRemittancesID', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneSecondName', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneficiaryAmount', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('beneficiaryCurrency', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('countryFrom', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('date', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('dateTimeStamp', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('description', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                ]), True),
                T.StructField('exchangeRate', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('fullResponse', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('mobileWalletAccountNo', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('mobileWalletPrefix', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('orderNo', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('payerDocumentId', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('payerDocumentType', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('payerFirstName', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('payerLastName', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('payerLastName2', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('payerRemittancesId', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('payerSecondName', T.StructType([
                    T.StructField('NULL', T.BooleanType(), True),
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('paymentAmount', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('paymentCurrency', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('remittanceId', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('salesDate', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
            ]), True),
        ]), True),
        T.StructField('orderId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('phoneNumber', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('remittanceId', T.StructType([
            T.StructField('S', T.StringType(), True),
        ]), True),
        T.StructField('state', T.StructType([
            T.StructField('M', T.StructType([
                T.StructField('cause', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('code', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('date', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('dateTStamp', T.StructType([
                    T.StructField('N', T.StringType(), True),
                ]), True),
                T.StructField('orderSteps', T.StructType([
                    T.StructField('L', T.ArrayType(
                        T.StructType([
                            T.StructField('M', T.StructType([
                                T.StructField('date', T.StructType([
                                    T.StructField('S', T.StringType(), True),
                                ]), True),
                                T.StructField('dateTStamp', T.StructType([
                                    T.StructField('N', T.StringType(), True),
                                ]), True),
                                T.StructField('orderStep', T.StructType([
                                    T.StructField('S', T.StringType(), True),
                                ]), True),
                            ]), True),
                        ])
                    , True), True),
                ]), True),
                T.StructField('status', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
                T.StructField('step', T.StructType([
                    T.StructField('S', T.StringType(), True),
                ]), True),
            ]), True),
        ]), True),
    ]), True),
])

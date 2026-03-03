# End-to-End Tests - Remittances Order Status

Tests de integración completos que validan el flujo ETL end-to-end para el proceso de DynamoDB curated.

## Ubicación

```
tests/examples/remittances_order_status/
├── conftest.py              # Fixtures compartidas (Spark, GlueContext)
├── test_etl_full.py         # Tests para proceso FULL
├── test_etl_incremental.py  # Tests para proceso INCREMENTAL
└── README.md                # Este archivo
```

## Tests Implementados

### test_etl_full.py
- **test_full_etl_pipeline**: Valida flujo completo FULL (Configuration → Extract → Transform → Load)
- **test_full_etl_pipeline_empty_data**: Valida manejo de datos vacíos en proceso FULL

### test_etl_incremental.py
- **test_incremental_etl_pipeline**: Valida flujo completo INCREMENTAL (Configuration → Extract → Transform → Load)
- **test_incremental_etl_pipeline_empty_data**: Valida manejo de datos vacíos en proceso INCREMENTAL
- **test_incremental_with_delete_events**: Valida manejo de eventos DELETE (sin NewImage)

## Ejecución

```bash
# Ejecutar todos los tests end-to-end
pytest tests/examples/remittances_order_status/ -v

# Ejecutar solo tests FULL
pytest tests/examples/remittances_order_status/test_etl_full.py -v

# Ejecutar solo tests INCREMENTAL
pytest tests/examples/remittances_order_status/test_etl_incremental.py -v

# Con cobertura
pytest tests/examples/remittances_order_status/ --cov=examples.co_delfos_productos_srf_transformar_remittances_order_status --cov-report=term-missing
```

## Cobertura

Los tests cubren:
- ✅ Configuration (JobConfig, Constants, JobParameters)
- ✅ Extract (Catalog, RawSources)
- ✅ Transform (FlattenTransformations, DynamoDBTableTransformations)
- ✅ Metadata (MetadataConfig)
- ✅ Validación de datos vacíos
- ✅ Validación de estructura final del DataFrame
- ✅ Diferencias entre FULL e INCREMENTAL (Item vs NewImage, particiones)

## Mocks Utilizados

- **Catalog._read_from_s3**: Mock para evitar lectura real de S3
- **MetadataConfig.load_metadata**: Mock para evitar lectura de archivos YAML
- **JobParameters**: Mock para simular parámetros de entrada

## Datos de Prueba

### FULL
- Estructura: `{"Item": {"order_id": {"S": "..."}, ...}}`
- 2 registros de prueba con PKs válidos

### INCREMENTAL
- Estructura: `{"Keys": {...}, "NewImage": {...}, "eventName": "..."}`
- 2 registros de prueba (MODIFY, INSERT)
- 1 registro DELETE para validar manejo de eventos sin NewImage

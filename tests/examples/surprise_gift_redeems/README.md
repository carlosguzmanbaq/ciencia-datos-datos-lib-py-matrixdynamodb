# Tests: Surprise Gift Redeems Module

Tests end-to-end para el módulo `co_delfos_productos_srf_transformar_surprise_gift_redeems`.

## 📋 Estructura

```
tests/examples/surprise_gift_redeems/
├── __init__.py
├── conftest.py              # Fixtures específicas del módulo
├── test_etl_full.py        # Tests FULL
├── test_etl_incremental.py # Tests INC
└── README.md               # Este archivo
```

## 🎯 Características Testeadas

### Pipeline ETL Completo
- ✅ **FULL**: Carga completa de tabla
- ✅ **INC**: Carga incremental (INSERT/MODIFY)
- ✅ Flatten de estructura DynamoDB
- ✅ Transformaciones de negocio
- ✅ Agregado de metadata
- ✅ Manejo de datos vacíos

### Primary Keys
- ✅ Múltiples PKs: `clientId` + `tstamp`
- ✅ Validación de extracción de PKs

### Casos Edge
- ✅ Datos vacíos (FULL e INC)
- ✅ Validación de columnas obligatorias
- ✅ Tipos de datos mixtos (N y S en campo `value`)

## 🧪 Ejecutar Tests

```bash
# Todos los tests del módulo
pytest tests/examples/surprise_gift_redeems/ -v

# Solo FULL
pytest tests/examples/surprise_gift_redeems/test_etl_full.py -v

# Solo INCREMENTAL
pytest tests/examples/surprise_gift_redeems/test_etl_incremental.py -v

# Con cobertura
pytest tests/examples/surprise_gift_redeems/ \
  --cov=examples.co_delfos_productos_srf_transformar_surprise_gift_redeems \
  --cov-report=term-missing

# Con cobertura HTML
pytest tests/examples/surprise_gift_redeems/ \
  --cov=examples.co_delfos_productos_srf_transformar_surprise_gift_redeems \
  --cov-report=html
```

## 📦 Fixtures

### Compartidas (desde tests/conftest.py)
- `spark`: Sesión Spark para todos los tests
- `glue_context`: Mock de GlueContext

### Locales (conftest.py)
- `constants`: Configuración del dominio surprise_gift_redeems
- `job_params_full`: Parámetros FULL con `event_status=False`
- `job_params_inc`: Parámetros INC con `event_status=False`

### Por Test
- `mock_dynamodb_full_data`: Datos FULL de DynamoDB
- `mock_dynamodb_incremental_data`: Datos INC con INSERT/MODIFY

## 🔍 Tests Detallados

### test_etl_full.py

#### `test_full_etl_pipeline`
- Procesa 2 registros FULL
- Valida flatten de estructura DynamoDB
- Confirma transformaciones de negocio
- Verifica columnas de metadata

#### `test_full_etl_pipeline_empty_data`
- Maneja DataFrame vacío correctamente
- Retorna `(None, None, None, 0)`

### test_etl_incremental.py

#### `test_incremental_etl_pipeline`
- Procesa 2 registros INC (MODIFY + INSERT)
- Valida extracción de PKs múltiples
- Confirma transformaciones de negocio
- Verifica columnas de metadata

#### `test_incremental_empty_data`
- Maneja DataFrame vacío en INC
- Retorna `(None, None, None, 0)`

## 🎨 Datos de Prueba

### FULL Export
```python
{
    "clientId": "CLIENT001",
    "tstamp": "1705320000000",
    "documentNumber": "123456789",
    "documentType": "CC",
    "giftCode": "GIFT123",
    "merchantId": "MERCHANT001",
    "phoneNumber": "+573001234567",
    "transactionId": "TXN001",
    "value": "50000"  # Puede ser N o S
}
```

### INCREMENTAL Export
```python
# MODIFY
{
    "Keys": {
        "clientId": "CLIENT001",
        "tstamp": "1705320000000"
    },
    "NewImage": {...},  # Datos completos
    "eventName": "MODIFY"
}

# INSERT
{
    "Keys": {
        "clientId": "CLIENT003",
        "tstamp": "1705326000000"
    },
    "NewImage": {...},  # Datos completos
    "eventName": "INSERT"
}
```

## 🔧 Mocks Utilizados

### GlueContext
```python
glue_context.create_data_frame.from_options = Mock(return_value=mock_df)
```

### Metadata
```python
with patch('dynamodb_curated_library.metadata.metadata.MetadataConfig.load_metadata') as mock_load:
    mock_load.return_value = {}
```

## ✅ Validaciones Clave

### Columnas Obligatorias
```python
assert "client_id" in df_flatten.columns
assert "tstamp" in df_flatten.columns
assert "momento_ingestion" in df_final.columns
assert "job_process_date" in df_final.columns
```

### Conteo de Registros
```python
assert record_count == 2
assert df_transform.count() == 2
```

### Datos Vacíos
```python
assert result == (None, None, None, 0)
```

## 🚀 Diferencias con Otros Módulos

| Aspecto             | surprise_gift_redeems        | customerknowid_module |
| ------------------- | ---------------------------- | --------------------- |
| **event_status**    | False (deshabilitado)        | True (habilitado)     |
| **Primary Keys**    | Múltiples (clientId, tstamp) | Single (customerId)   |
| **DELETE Events**   | ❌ No soportado               | ✅ Soportado           |
| **Partition Field** | tstamp                       | creation_date         |
| **Domain**          | productos                    | clientes              |

## 📊 Cobertura Esperada

- **Mínima**: 85%
- **Objetivo**: 90%+
- **Crítica (100%)**:
  - Pipeline ETL completo
  - Flatten de estructura
  - Manejo de datos vacíos

## 🤝 Contribución

Al agregar nuevos tests:
1. Usar fixtures compartidas cuando sea posible
2. Documentar casos edge específicos
3. Validar columnas obligatorias
4. Mockear dependencias apropiadamente
5. Mantener consistencia con otros módulos

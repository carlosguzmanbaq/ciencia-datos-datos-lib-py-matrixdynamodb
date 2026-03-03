# Tests: Customer Know ID Module

Tests end-to-end para el módulo `co_delfos_clientes_prn_transformar_customerknowid_module` con **event_status habilitado**.

## 📋 Estructura

```
tests/examples/customerknowid_module/
├── __init__.py
├── conftest.py                  # Fixtures específicas del módulo
├── test_etl_full.py            # Tests FULL con event_status=True
├── test_etl_incremental.py     # Tests INC con INSERT/UPDATE/DELETE
└── README.md                   # Este archivo
```

## 🎯 Características Testeadas

### Event Status Tracking
- ✅ **FULL**: Todos los registros → `flag_event_status = "INSERT"`
- ✅ **INC - INSERT**: Keys + NewImage, PK no existe → `flag_event_status = "INSERT"`
- ✅ **INC - UPDATE**: Keys + NewImage, PK existe → `flag_event_status = "UPDATE"`
- ✅ **INC - DELETE**: Keys sin NewImage, PK existe → `flag_event_status = "DELETE"`

### Enriquecimiento de DELETE
- ✅ Recupera datos completos del catálogo para registros DELETE
- ✅ Preserva `flag_event_status = "DELETE"` después del enriquecimiento

### Casos Edge
- ✅ Manejo de datos vacíos
- ✅ Validación de columnas de metadata
- ✅ Transformaciones de negocio personalizadas

## 🧪 Ejecutar Tests

```bash
# Todos los tests del módulo
pytest tests/examples/customerknowid_module/ -v

# Solo FULL
pytest tests/examples/customerknowid_module/test_etl_full.py -v

# Solo INCREMENTAL
pytest tests/examples/customerknowid_module/test_etl_incremental.py -v

# Con cobertura
pytest tests/examples/customerknowid_module/ --cov=examples.co_delfos_clientes_prn_transformar_customerknowid_module --cov-report=term-missing

# Con cobertura HTML
pytest tests/examples/customerknowid_module/ --cov=examples.co_delfos_clientes_prn_transformar_customerknowid_module --cov-report=html
```

## 📦 Fixtures

### Compartidas (desde tests/conftest.py)
- `spark`: Sesión Spark para todos los tests
- `glue_context`: Mock de GlueContext

### Locales (conftest.py)
- `constants`: Configuración del dominio customerknowid
- `job_params_full`: Parámetros FULL con `event_status=True`
- `job_params_inc`: Parámetros INC con `event_status=True`

### Por Test
- `mock_dynamodb_full_data`: Datos FULL de DynamoDB
- `mock_dynamodb_incremental_data`: Datos INC con INSERT/UPDATE
- `mock_existing_curated_table`: Tabla curada existente para comparación

## 🔍 Tests Detallados

### test_etl_full.py

#### `test_full_etl_pipeline_with_event_status`
- Procesa 2 registros FULL
- Verifica que todos tengan `flag_event_status = "INSERT"`
- Valida transformaciones de negocio
- Confirma columnas de metadata

#### `test_full_etl_pipeline_empty_data`
- Maneja DataFrame vacío correctamente
- Retorna `(None, None, None, 0)`

### test_etl_incremental.py

#### `test_incremental_etl_with_event_status`
- **CUST001**: Existe en tabla → `flag_event_status = "UPDATE"`
- **CUST003**: No existe → `flag_event_status = "INSERT"`
- Mock de lectura del catálogo para comparación

#### `test_incremental_with_delete_events`
- Procesa evento DELETE (Keys sin NewImage)
- Verifica `flag_event_status = "DELETE"`
- Valida enriquecimiento con datos del catálogo
- Confirma que el registro DELETE tiene datos de negocio completos

#### `test_incremental_empty_data`
- Maneja DataFrame vacío en INC
- Retorna `(None, None, None, 0)`

## 🎨 Datos de Prueba

### FULL Export
```python
{
    "customerId": "CUST001",
    "email": "john@example.com",
    "firstName": "John",
    "products": {
        "credits": true,
        "remittances": true
    },
    "notifications": [...]
}
```

### INCREMENTAL Export
```python
# INSERT
{
    "Keys": {"customerId": "CUST003"},
    "NewImage": {...},  # Datos completos
    "eventName": "INSERT"
}

# UPDATE
{
    "Keys": {"customerId": "CUST001"},
    "NewImage": {...},  # Datos actualizados
    "eventName": "MODIFY"
}

# DELETE
{
    "Keys": {"customerId": "CUST002"},
    "NewImage": null,  # Sin datos
    "eventName": "REMOVE"
}
```

## 🔧 Mocks Utilizados

### GlueContext
```python
glue_context.create_data_frame.from_options = Mock(return_value=mock_df)
```

### Spark Read (para event_status)
```python
with patch.object(spark, 'read') as mock_read:
    mock_read.table.return_value = mock_existing_curated_table
```

### Catalog Read (para enrichment)
```python
with patch.object(catalog, 'read_table', return_value=mock_enriched_df):
    # Enriquecimiento de DELETE
```

## ✅ Validaciones Clave

### Event Status
```python
assert event_statuses["CUST001"] == "UPDATE"
assert event_statuses["CUST003"] == "INSERT"
assert event_status == "DELETE"
```

### Columnas Obligatorias
```python
assert "flag_event_status" in df_flatten.columns
assert "momento_ingestion" in df_final.columns
assert "job_process_date" in df_final.columns
```

### Enriquecimiento DELETE
```python
delete_record = df_transform.filter(F.col("flag_event_status") == "DELETE").first()
assert delete_record.customer_id == "CUST002"
assert delete_record.email == "jane@example.com"  # Datos del catálogo
```

## 🚀 Diferencias con remittances_order_status

### Centralización
- ✅ Usa `tests/conftest.py` compartido (no duplicado)
- ✅ Solo fixtures específicas en conftest.py local

### Event Status
- ✅ `event_status=True` en todos los tests
- ✅ Tests específicos para INSERT/UPDATE/DELETE
- ✅ Validación de enriquecimiento de DELETE

### Primary Key
- ✅ Single PK: `customerId` (vs múltiples PKs en remittances)

## 📊 Cobertura Esperada

- **Mínima**: 90%
- **Objetivo**: 95%+
- **Crítica (100%)**:
  - Event status logic
  - DELETE enrichment
  - Business transformations

## 🤝 Contribución

Al agregar nuevos tests:
1. Usar fixtures compartidas cuando sea posible
2. Documentar casos edge específicos
3. Validar `flag_event_status` en todos los escenarios
4. Mockear lecturas del catálogo apropiadamente

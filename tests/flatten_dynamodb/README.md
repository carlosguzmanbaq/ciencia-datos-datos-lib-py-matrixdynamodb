# Tests de flatten_dynamodb_struct

Suite de tests organizados por features para validar la funcionalidad de `flatten_dynamodb_struct`.

## Estructura de Tests

```
tests/flatten_dynamodb/
├── conftest.py                      # Fixtures compartidas (Spark session)
├── test_helper_functions.py         # Tests de funciones auxiliares
├── test_basic_flattening.py         # Tests de aplanado básico
├── test_depth_control.py            # Tests de control de profundidad
├── test_columns_as_json.py          # Tests de conversión manual a JSON
├── test_arrays.py                   # Tests de manejo de arrays
├── test_error_handling.py           # Tests de manejo de errores
└── test_edge_cases.py               # Tests de casos especiales
```

---

## Ejecutar Tests

### Comandos Básicos

```bash
# Todos los tests de flatten
pytest tests/flatten_dynamodb/ -v

# Un archivo específico
pytest tests/flatten_dynamodb/test_basic_flattening.py -v

# Un test específico
pytest tests/flatten_dynamodb/test_basic_flattening.py::TestBasicFlattening::test_simple_structure -v
```

### Con Cobertura

```bash
# Cobertura con reporte en terminal
pytest tests/flatten_dynamodb/ \
  --cov=dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct \
  --cov-report=term-missing

# Cobertura con reporte HTML
pytest tests/flatten_dynamodb/ \
  --cov=dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct \
  --cov-report=html

# Ver reporte HTML
open htmlcov/index.html
```

### Opciones Útiles

```bash
# Modo verbose con output completo
pytest tests/flatten_dynamodb/ -vv -s

# Solo tests que fallaron en la última ejecución
pytest tests/flatten_dynamodb/ --lf

# Detener en el primer fallo
pytest tests/flatten_dynamodb/ -x

# Ejecutar en paralelo (requiere pytest-xdist)
pytest tests/flatten_dynamodb/ -n auto
```

---

## Descripción de Tests por Archivo

### 1. `test_helper_functions.py`

**Propósito**: Validar funciones auxiliares de transformación.

**Tests incluidos**:
- `test_to_snake_case`: Conversión de camelCase a snake_case
  - `orderId` → `order_id`
  - `remittanceId` → `remittance_id`

- `test_calculate_depth`: Cálculo de profundidad excluyendo marcadores DynamoDB
  - `state.M.orderId.S` → depth 2
  - `state.M.orderSteps.M.items` → depth 3

**Cobertura**: Funciones `to_snake_case()` y `calculate_depth()`

---

### 2. `test_basic_flattening.py`

**Propósito**: Validar aplanado básico de estructuras DynamoDB.

**Tests incluidos**:

#### TestBasicFlattening
- `test_simple_structure`: Estructura simple con tipos S y N
- `test_nested_structure_level_2`: Estructura anidada de 2 niveles

#### TestItemParentCol
- `test_item_parent_col_simple`: Export FULL con columna Item (simple)
- `test_item_parent_col_nested`: Export FULL con estructuras anidadas
- `test_item_with_depth_control`: Item con control de profundidad
- `test_item_with_columns_as_json`: Item con conversión manual a JSON

**Cobertura**:
- Aplanado básico de estructuras simples y anidadas
- Diferencia entre FULL (Item) e INCREMENTAL (NewImage)
- Conversión automática a snake_case

---

### 3. `test_depth_control.py`

**Propósito**: Validar control de profundidad con `max_depth`.

**Tests incluidos**:

#### TestMaxDepthControl
- `test_max_depth_2`: Estructuras profundas convertidas a JSON con max_depth=2
- `test_max_depth_1`: Nivel 2+ convertido a JSON con max_depth=1

#### TestCombinedScenarios
- `test_max_depth_and_columns_as_json`: Combinación de ambas estrategias

#### TestShouldConvertToJsonLogic
- `test_columns_as_json_exact_match`: Match exacto en columns_as_json
- `test_columns_as_json_prefix_match`: Match por prefijo
- `test_depth_exceeds_max_depth_returns_true`: Profundidad excede max_depth
- `test_return_false_when_no_conditions_met`: Sin condiciones cumplidas

**Cobertura**:
- Parámetro `max_depth`
- Lógica de `should_convert_to_json()`
- Conversión automática a JSON por profundidad

---

### 4. `test_columns_as_json.py`

**Propósito**: Validar conversión manual de columnas a JSON.

**Tests incluidos**:
- `test_specific_column_as_json`: Columna específica como JSON
- `test_multiple_columns_as_json`: Múltiples columnas como JSON

**Cobertura**:
- Parámetro `columns_as_json`
- Control manual sobre qué aplanar
- Conversión selectiva a JSON

---

### 5. `test_arrays.py`

**Propósito**: Validar manejo de arrays de DynamoDB.

**Tests incluidos**:

#### TestArraysWithoutMWrapper
- `test_array_without_m_wrapper`: Arrays sin wrapper M (StructType directo)
- `test_array_with_mixed_field_types`: Arrays con tipos mixtos (S y N)
- `test_array_with_simple_types`: Arrays con tipos simples (no StructType)
- `test_empty_array`: Manejo de arrays vacíos
- `test_array_elements_with_no_fields`: StructType sin campos

**Cobertura**:
- Arrays con wrapper M de DynamoDB
- Arrays sin wrapper M
- Función `extract_fields_from_array()`
- Función `generate_transform_expr()`

---

### 6. `test_error_handling.py`

**Propósito**: Validar manejo de errores y excepciones personalizadas.

**Tests incluidos**:
- `test_error_handling_raises_custom_exception`: Lanza FlattenStructureException
- `test_error_message_includes_parent_col`: Mensaje incluye parent_col
- `test_exception_includes_suggestions`: Excepción incluye sugerencias

**Cobertura**:
- Excepción `FlattenStructureException`
- Mensajes de error con contexto
- Sugerencias de resolución

---

### 7. `test_edge_cases.py`

**Propósito**: Validar casos especiales y escenarios de producción.

**Tests incluidos**:

#### TestComplexErrorScenarios
- `test_varying_schema_between_records`: Schemas variables entre registros
- `test_null_values_in_nested_structures`: Valores NULL en estructuras anidadas
- `test_empty_structures`: Estructuras vacías (M: {})
- `test_mixed_types_in_same_field`: Mismo campo con tipos diferentes (S vs N)
- `test_deeply_nested_with_columns_as_json_recovery`: Recuperación con columns_as_json

**Cobertura**:
- Casos edge comunes en DynamoDB
- Manejo de nulls y estructuras vacías
- Tipos mixtos y schemas variables

---

## Features Cubiertas

### ✅ Funcionalidad Core
- [x] Aplanado básico de estructuras DynamoDB
- [x] Conversión automática a snake_case
- [x] Manejo de tipos DynamoDB (M, L, S, N, BOOL, NULL)
- [x] Soporte para FULL (Item) e INCREMENTAL (NewImage)

### ✅ Control de Profundidad
- [x] Parámetro `max_depth`
- [x] Conversión automática a JSON por profundidad
- [x] Lógica de `should_convert_to_json()`

### ✅ Conversión Manual
- [x] Parámetro `columns_as_json`
- [x] Match exacto y por prefijo
- [x] Combinación con max_depth

### ✅ Manejo de Arrays
- [x] Arrays con wrapper M
- [x] Arrays sin wrapper M
- [x] Arrays vacíos
- [x] Tipos mixtos en arrays

### ✅ Manejo de Errores
- [x] Excepción personalizada `FlattenStructureException`
- [x] Mensajes con contexto (max_depth, parent_col)
- [x] Sugerencias de resolución

### ✅ Edge Cases
- [x] Schemas variables entre registros
- [x] Valores NULL
- [x] Estructuras vacías
- [x] Tipos mixtos
- [x] Estructuras muy profundas

---

## Fixtures Compartidas

### `spark` (conftest.py)

Fixture de módulo que proporciona una sesión de Spark para todos los tests.

```python
@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .appName("test_flatten_dynamodb") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()
```

**Scope**: `module` - Se crea una vez por archivo de test
**Uso**: Inyectado automáticamente en tests que lo requieren

---

## Convenciones de Testing

### Nomenclatura
- Clases de test: `TestFeatureName`
- Métodos de test: `test_specific_scenario`
- Fixtures: `snake_case`

### Estructura de Tests
```python
def test_scenario_name(self, spark):
    """Descripción clara del escenario."""
    # 1. Arrange - Preparar datos
    data = [...]
    schema = T.StructType([...])
    df = spark.createDataFrame(data, schema)

    # 2. Act - Ejecutar función
    result = flatten_dynamodb_struct(df, ...)

    # 3. Assert - Verificar resultados
    assert result.count() == expected
    assert "column_name" in result.columns
```

### Assertions Comunes
```python
# Verificar columnas
assert "column_name" in result.columns

# Verificar valores
row = result.first()
assert row.column_name == "expected_value"

# Verificar tipos
assert isinstance(row.column_name, str)

# Verificar excepciones
with pytest.raises(FlattenStructureException) as exc_info:
    flatten_dynamodb_struct(df, ...)
assert "expected text" in str(exc_info.value)
```

---

## Métricas de Cobertura

### Objetivo
- **Cobertura mínima**: 90%
- **Cobertura objetivo**: 95%+

### Áreas Críticas (100% cobertura)
- Funciones helper (`to_snake_case`, `calculate_depth`)
- Lógica de profundidad (`should_convert_to_json`)
- Manejo de errores (`FlattenStructureException`)
- Extracción de arrays (`extract_fields_from_array`)

---

## Troubleshooting

### Tests Fallan Localmente

```bash
# Verificar versión de PySpark
pip show pyspark

# Reinstalar dependencias
pip install -r requirements.txt

# Limpiar cache de pytest
pytest --cache-clear
```

### Problemas de Memoria

```bash
# Aumentar memoria de Spark en tests
export SPARK_DRIVER_MEMORY=2g
pytest tests/flatten_dynamodb/
```

### Tests Lentos

```bash
# Ejecutar solo tests rápidos (excluir slow)
pytest tests/flatten_dynamodb/ -m "not slow"

# Ejecutar en paralelo
pytest tests/flatten_dynamodb/ -n 4
```

---

## Contribuir

### Agregar Nuevos Tests

1. Identificar el archivo correcto según la feature
2. Seguir convenciones de nomenclatura
3. Incluir docstring descriptivo
4. Verificar cobertura: `pytest --cov`

### Ejemplo de Nuevo Test

```python
def test_new_scenario(self, spark):
    """Test description of what this validates."""
    # Arrange
    data = [{"NewImage": {"field": {"S": "value"}}}]
    schema = T.StructType([...])
    df = spark.createDataFrame(data, schema)

    # Act
    result = flatten_dynamodb_struct(df)

    # Assert
    assert result.count() == 1
    assert "field" in result.columns
```

---

## Referencias

- [Documentación de flatten_dynamodb_struct](../../dynamodb_curated_library/etl/transform/utils/README.md)
- [Pytest Documentation](https://docs.pytest.org/)
- [PySpark Testing Best Practices](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)

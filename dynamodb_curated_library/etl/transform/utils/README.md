# flatten_dynamodb_struct

Función para aplanar estructuras anidadas de DynamoDB exportadas a S3 en un DataFrame normalizado de PySpark.

## Descripción

`flatten_dynamodb_struct` convierte el formato jerárquico de DynamoDB (con tipos M, L, S, N, BOOL) en columnas planas con nombres en snake_case, facilitando el análisis y almacenamiento en formatos como Apache Hudi.

## Firma de la Función

```python
def flatten_dynamodb_struct(
    df: DataFrame,
    parent_col: str = "NewImage",
    max_depth: int = 3,
    columns_as_json: Optional[List[str]] = None
) -> DataFrame
```

## Parámetros

| Parámetro         | Tipo      | Default    | Descripción                                                                           |
| ----------------- | --------- | ---------- | ------------------------------------------------------------------------------------- |
| `df`              | DataFrame | -          | DataFrame de PySpark con estructura anidada de DynamoDB                               |
| `parent_col`      | str       | "NewImage" | Columna raíz que contiene los datos ("Item" para FULL, "NewImage" para INCREMENTAL)   |
| `max_depth`       | int       | 3          | Profundidad máxima de aplanado. Estructuras más profundas se convierten a JSON string |
| `columns_as_json` | List[str] | None       | Lista de paths de columnas a mantener como JSON string en lugar de aplanar            |

## Retorno

`DataFrame`: DataFrame con estructura aplanada de DynamoDB.

## Excepciones

`FlattenStructureException`: Si el aplanado falla, incluye sugerencias de configuración para resolver el problema.

---

## Estrategia de Resolución de Conflictos

### Problema Resuelto

Cuando un campo DynamoDB tiene múltiples tipos (ej: `credits.S` y `credits.BOOL`), las estrategias tradicionales de priorización causan **pérdida de datos**:

```python
# Ejemplo real:
# - 87,409 registros con BOOL=false
# - 5,111 registros con BOOL=true
# - 1 registro con S="1"
# - 92,520 registros con S=null

# Estrategia anterior (priorización):
# → Solo preservaba 1 registro (S), perdía 92,520 registros con BOOL

# Nueva estrategia (híbrida):
# → Preserva todos: 87,409 + 5,111 + 1 = 92,521 registros
```

### Solución Implementada

#### 1. Tipos Estructurales: Priorización
```python
# Para L (Array) y M (Map) - definen estructura de datos
if highest_priority_type == "L":
    # Procesar como array (ignora otros tipos)
elif highest_priority_type == "M":
    # Procesar como estructura anidada
```

**Razón**: Un campo no puede ser simultáneamente array y mapa.

#### 2. Tipos de Valor: Coalesce
```python
# Para S, N, BOOL - preserva todos los valores no-null
coalesce(field.S, field.N, field.BOOL)
```

**Razón**: Todos los tipos de valor pueden coexistir y deben preservarse.

### Casos de Uso de Conflictos

#### Caso 1: Array vs NULL
```python
# notifications.L = [{"messageId": "123"}]
# notifications.NULL = true

# Resultado: Procesa como array (L prioridad)
# notifications = [{"message_id": "123"}]
```

#### Caso 2: String vs Boolean
```python
# status.S = null
# status.BOOL = true

# Resultado: Usa coalesce
# status = "true"  # BOOL convertido a string
```

#### Caso 3: Múltiples Valores
```python
# credits.S = "premium"
# credits.BOOL = false

# Resultado: S toma precedencia en coalesce
# credits = "premium"
```

#### Caso 4: Solo BOOL Disponible
```python
# credits.S = null
# credits.BOOL = true

# Resultado: Preserva BOOL
# credits = "true"
```

### Logging de Conflictos

La función registra conflictos detectados:
```python
logger.debug("Conflict detected for %s: %s. Using highest priority: %s",
           "notifications", ["L", "NULL"], "L")
```

**Beneficio**: Visibilidad de qué decisiones toma la función para debugging.

---

## Casos de Uso

### 1. Uso Básico - Export INCREMENTAL (Default)

Para exports incrementales de DynamoDB que usan la columna `NewImage`:

```python
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import flatten_dynamodb_struct

# DataFrame con estructura DynamoDB incremental
# NewImage.orderId.S = "123"
# NewImage.amount.N = "100"

flatten_df = flatten_dynamodb_struct(df)

# Resultado:
# order_id | amount
# ---------|-------
# "123"    | "100"
```

**Características:**
- Usa `parent_col="NewImage"` por defecto
- Convierte nombres a snake_case automáticamente
- Aplana hasta 3 niveles de profundidad

---

### 2. Export FULL - Usando Columna "Item"

Para exports completos de DynamoDB que usan la columna `Item`:

```python
# DataFrame con estructura DynamoDB FULL
# Item.orderId.S = "123"
# Item.customer.M.name.S = "John Doe"

flatten_df = flatten_dynamodb_struct(
    df,
    parent_col="Item"
)

# Resultado:
# order_id | customer_name
# ---------|---------------
# "123"    | "John Doe"
```

**Cuándo usar:**
- Exports FULL de DynamoDB
- Cuando la columna raíz es `Item` en lugar de `NewImage`

---

### 3. Control de Profundidad - Evitar OOM en Estructuras Complejas

Cuando tienes estructuras muy anidadas que pueden causar problemas de memoria:

```python
# Estructura con 5 niveles de profundidad
# NewImage.level1.M.level2.M.level3.M.level4.M.level5.S = "deep"

flatten_df = flatten_dynamodb_struct(
    df,
    max_depth=2  # Solo aplana hasta nivel 2
)

# Resultado:
# level1_level2 (JSON string)
# --------------
# '{"M":{"level3":{"M":{"level4":...}}}}'
```

**Cuándo usar:**
- Datasets grandes que causan Out of Memory (OOM)
- Estructuras con más de 3-4 niveles de anidación
- Cuando solo necesitas los primeros niveles aplanados

**Recomendaciones:**
- FULL exports: `max_depth=2` o `max_depth=3`
- INCREMENTAL exports: `max_depth=3` (default)

---

### 4. Conversión Manual a JSON - Columnas Específicas

Cuando quieres mantener ciertas columnas como JSON sin aplanar:

```python
# Estructura con múltiples columnas anidadas
# NewImage.orderId.S = "123"
# NewImage.state.M.orderSteps.M.step1.S = "completed"
# NewImage.metadata.M.tags.L = [...]

flatten_df = flatten_dynamodb_struct(
    df,
    columns_as_json=["state.M.orderSteps", "metadata"]
)

# Resultado:
# order_id | state_order_steps (JSON) | metadata (JSON)
# ---------|--------------------------|----------------
# "123"    | '{"M":{"step1":...}}'    | '{"M":{"tags":...}}'
```

**Cuándo usar:**
- Columnas con estructura variable entre registros
- Datos que prefieres consultar como JSON
- Columnas que causan errores al aplanar

**Ventajas:**
- Control granular sobre qué aplanar
- Evita errores en columnas problemáticas
- Mantiene flexibilidad en datos variables

---

### 5. Combinación - Profundidad + Columnas Específicas

Combina ambas estrategias para máximo control:

```python
flatten_df = flatten_dynamodb_struct(
    df,
    parent_col="Item",
    max_depth=2,
    columns_as_json=["state.M.orderSteps", "config"]
)
```

**Cuándo usar:**
- Datasets grandes con columnas problemáticas específicas
- Necesitas control fino sobre el aplanado
- Optimización de performance y memoria

---

## Manejo de Errores

Si el aplanado falla, recibirás una excepción con sugerencias:

```
================================================================================
ERROR: Failed to flatten DynamoDB structure
================================================================================
Details: The structure might be too complex or vary between records
Caused by: [FIELD_NOT_FOUND] No such struct field...

SUGGESTIONS:
  1. Reduce max_depth parameter (current: 3)
  2. Add problematic columns to 'columns_as_json' parameter
  3.
Example:
  flatten_dynamodb_struct(
      df,
      parent_col='NewImage',
      max_depth=2,
      columns_as_json=['state.M.orderSteps', 'metadata.M.complex']
  )
================================================================================
```

**Pasos para resolver:**
1. Identifica la columna problemática en el mensaje de error
2. Agrégala a `columns_as_json`
3. O reduce `max_depth` si es una estructura muy profunda

---

## Características Adicionales

### Conversión Automática de Nombres

Todos los nombres de columnas se convierten automáticamente a snake_case:

```python
# DynamoDB: orderId, customerName, orderDate
# Resultado: order_id, customer_name, order_date
```

### Soporte de Arrays

Maneja arrays de DynamoDB con y sin wrapper 'M':

```python
# Con wrapper M:
# items.L[0].M.id.S = "item1"

# Sin wrapper M:
# tags.L[0].id.S = "tag1"

# Ambos son procesados correctamente
```

### Manejo de Tipos DynamoDB

Extrae automáticamente valores de tipos DynamoDB usando una **estrategia híbrida**:

#### Tipos Estructurales (Priorización)
Para tipos que definen estructura de datos:
- **L (Array)**: Prioridad máxima - procesa como array
- **M (Map)**: Segunda prioridad - procesa como estructura anidada

#### Tipos de Valor (Coalesce)
Para tipos que contienen valores, usa **coalesce** para preservar todos los datos:
```python
# Orden de coalesce: S → N → BOOL
# Preserva el primer valor no-null encontrado

# Caso 1: Solo BOOL tiene valor
field.BOOL = true, field.S = null  → "true"

# Caso 2: Solo S tiene valor
field.S = "active", field.BOOL = null  → "active"

# Caso 3: Ambos tienen valor (S toma precedencia)
field.S = "override", field.BOOL = true  → "override"
```

**Ventaja**: No se pierden datos valiosos. En lugar de descartar 92,520 registros con valores BOOL, se preservan todos los valores no-null.

#### Conflictos de Tipos
Cuando un campo tiene múltiples tipos DynamoDB:
```python
# notifications.L vs notifications.NULL
# → Usa L (array) por prioridad estructural

# credits.S vs credits.BOOL
# → Usa coalesce(S, BOOL) para preservar ambos valores
```

---

## Ejemplos Completos

### Ejemplo 1: Pipeline ETL Completo

```python
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import flatten_dynamodb_struct

# 1. Leer datos de DynamoDB export
df = spark.read.json("s3://bucket/dynamodb-export/")

# 2. Aplanar estructura
flatten_df = flatten_dynamodb_struct(
    df,
    parent_col="Item",  # FULL export
    max_depth=3
)

# 3. Escribir a Hudi
flatten_df.write.format("hudi") \
    .options(**hudi_options) \
    .save("s3://bucket/curated/")
```

### Ejemplo 2: Debugging - Identificar Profundidad

```python
# Probar con diferentes profundidades para encontrar el óptimo
for depth in [1, 2, 3, 4]:
    try:
        result = flatten_dynamodb_struct(df, max_depth=depth)
        print(f"max_depth={depth}: {result.count()} rows, {len(result.columns)} columns")
    except Exception as e:
        print(f"max_depth={depth}: Failed - {str(e)[:100]}")
```

### Ejemplo 3: Incremental con Columnas Problemáticas

```python
# Export incremental con columnas complejas
flatten_df = flatten_dynamodb_struct(
    df,
    parent_col="NewImage",  # Incremental
    max_depth=3,
    columns_as_json=[
        "state.M.orderSteps",      # Estructura variable
        "metadata.M.customFields"  # Campos dinámicos
    ]
)
```

---

## Performance Tips

1. **FULL Exports**: Usa `max_depth=2` a  `max_depth=3` para datasets grandes (>1M registros)
2. **Columnas Variables**: Agrégalas a `columns_as_json` en lugar de aumentar `max_depth`
3. **Arrays Grandes**: Considera mantenerlos como JSON si tienen muchos elementos
4. **Testing**: Prueba con una muestra pequeña primero para ajustar parámetros

---

## Ver También

- [Helper Functions](./helper_functions.md) - `to_snake_case()`, `calculate_depth()`
- [Exceptions](./exceptions.md) - `FlattenStructureException`
- [Tests](../../tests/flatten_dynamodb/) - Ejemplos de uso en tests

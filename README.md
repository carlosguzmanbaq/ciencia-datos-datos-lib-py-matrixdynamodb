# DynamoDB Curated Library

Librería Python para AWS Glue que simplifica el procesamiento ETL de datos exportados desde DynamoDB hacia un formato curado y optimizado usando PySpark y Apache Hudi.

## 🎯 ¿Qué Problemas Resuelve?

### 1. **Complejidad de Estructura DynamoDB**
DynamoDB exporta datos en un formato JSON anidado con tipos explícitos (`{"S": "value"}`, `{"N": "123"}`, `{"M": {...}}`). Esta librería:
- ✅ Aplana automáticamente estructuras anidadas complejas
- ✅ Convierte tipos DynamoDB a tipos nativos de Spark
- ✅ Maneja arrays con y sin wrapper 'M'
- ✅ Controla profundidad de aplanado para evitar explosión de columnas

### 2. **Gestión de Memoria (OOM)**
Procesar exports grandes de DynamoDB puede causar Out of Memory:
- ✅ Usa schemas predefinidos para FULL loads (evita inferencia costosa)
- ✅ Infiere schema solo para INCREMENTAL loads (datasets pequeños)
- ✅ Convierte estructuras profundas a JSON automáticamente

### 3. **Diferencias FULL vs INCREMENTAL**
Los exports de DynamoDB tienen estructuras diferentes:
- ✅ **FULL**: Procesa columna `Item`, formato fecha `YYYY-MM-DD`
- ✅ **INCREMENTAL**: Procesa columna `NewImage`, formato fecha `YYYY-MM-DD HH`
- ✅ Extrae primary keys dinámicamente según tipo de proceso

### 4. **Configuración Manual Repetitiva**
Cada tabla requiere configuración de buckets, paths, Hudi options:
- ✅ Genera automáticamente nombres de buckets según convenciones
- ✅ Configura Hudi options basado en primary keys
- ✅ Valida formatos de fecha según tipo de proceso

### 5. **Estabilidad de Schema en Glue Catalog**
Cambios en el schema pueden romper el catálogo:
- ✅ Función `align_schema()` mantiene schema estable
- ✅ Agrega columnas faltantes como NULL
- ✅ Ignora columnas nuevas no esperadas
- ✅ Castea tipos a schema almacenado

## 🚀 Características Principales

- **ETL Pipeline Completo**: Extract → Transform → Load con configuración mínima
- **Flatten Inteligente**: Aplanado automático con control de profundidad y conversión a JSON
- **Soporte Multi-PK**: Maneja múltiples primary keys dinámicamente
- **Metadata Automático**: Agrega columnas de auditoría (momento_ingestion, job_process_date, etc.)
- **Manejo de Errores**: Excepciones personalizadas con sugerencias contextuales
- **Logging Estructurado**: Logs informativos en cada etapa del proceso
- **Testing Completo**: 85%+ cobertura con tests unitarios y end-to-end

## 📦 Instalación

### Opción 1: Con Poetry (Recomendado)

#### Instalar Poetry
```bash
# macOS/Linux con Homebrew
brew install poetry

# Verificar instalación
poetry --version
```

#### Instalar Dependencias
```bash
# Clonar repositorio
git clone <repo>
cd ts_glue_curated_dynamodb_library

# Instalar dependencias (desarrollo local con PySpark)
poetry install -E spark

# O solo producción (sin PySpark, para AWS Glue)
poetry install
```

#### Gestionar Entorno Virtual

**Activar entorno:**
```bash
# Opción 1: Activar shell
source $(poetry env info --path)/bin/activate

# Opción 2: Ejecutar comandos sin activar
poetry run python script.py
poetry run pytest tests/ -v
```

**Ver información del entorno:**
```bash
# Ver path del entorno
poetry env info --path

# Ver lista de entornos
poetry env list
```

**Cambiar versión de Python:**
```bash
# Usar Python específico (3.9, 3.10 o 3.11)
poetry env use python3.10

# Reinstalar dependencias
poetry install -E spark
```

**Eliminar entorno:**
```bash
# Eliminar entorno actual
poetry env remove python3.14

# O eliminar por nombre
poetry env remove matrix-dynamodb-curated-library-xxxxx-py3.14

# Listar entornos disponibles
poetry env list
```

**Desactivar entorno:**
```bash
deactivate
```

#### Gestionar Dependencias

**Agregar dependencias:**
```bash
# Dependencia de producción
poetry add pandas

# Dependencia de desarrollo
poetry add --group dev black

# Con versión específica
poetry add boto3==1.40.23
```

**Actualizar dependencias:**
```bash
# Actualizar una dependencia
poetry update boto3

# Actualizar todas
poetry update

# Actualizar lock file sin instalar
poetry lock
```

**Ver dependencias:**
```bash
# Listar dependencias instaladas
poetry show

# Ver árbol de dependencias
poetry show --tree

# Ver dependencias desactualizadas
poetry show --outdated
```

**Remover dependencias:**
```bash
poetry remove pandas
```

#### Generar Archivo .whl para AWS Glue

**Empaquetar librería:**
```bash
# Generar .whl y .tar.gz
poetry build

# Resultado en dist/
# dist/matrix_dynamodb_curated_library-1.0.0-py3-none-any.whl
# dist/matrix-dynamodb-curated-library-1.0.0.tar.gz
```

**Subir a S3 y usar en Glue:**
```bash
# Subir a S3
aws s3 cp dist/matrix_dynamodb_curated_library-1.0.0-py3-none-any.whl \
  s3://tu-bucket/libs/

# En AWS Glue Job, agregar parámetro:
# --extra-py-files s3://tu-bucket/libs/matrix_dynamodb_curated_library-1.0.0-py3-none-any.whl
```

**Exportar requirements.txt (alternativa):**
```bash
# Generar requirements.txt desde Poetry
poetry export -f requirements.txt --output requirements.txt --without-hashes

# Solo producción (sin dev)
poetry export -f requirements.txt --output requirements.txt --only main --without-hashes
```

#### Comandos Útiles

```bash
# Verificar configuración
poetry check

# Limpiar cache
poetry cache clear pypi --all

# Ver configuración
poetry config --list
```

### Opción 2: Con pip (Tradicional)

```bash
pip install -r requirements.txt
```

### Requisitos

- **Python**: 3.9, 3.10 o 3.11 (compatible con PySpark 3.5.1)
- **PySpark**: 3.5.1
- **AWS Glue**: Compatible con Glue 4.0

### Mas información acerca de Poetry
- [GUIA-POETRY](https://grupobancolombia.visualstudio.com.mcas.ms/Nequi/_wiki/wikis/Nequi.wiki/474675/GUIA-POETRY)
- [PYPROJECT TOML EXPLAINED](https://grupobancolombia.visualstudio.com.mcas.ms/Nequi/_wiki/wikis/Nequi.wiki/474677/PYPROJECT-TOML-EXPLAINED)

## 🏗️ Arquitectura

```
Raw Data (S3) → Spark Processing → Curated Storage (Hudi) → Glue Catalog
     ↓                  ↓                    ↓
  DynamoDB         PySpark +            Optimized
   Export          Flatten             Columnar
```

## 📖 Uso Básico

### 1. Definir Configuración del Dominio

```python
from dynamodb_curated_library.core.config.constants import Constants

constants = Constants(
    domain="productos",
    subdomain="transacciones",
    data_product="remittances_order_status",
    country_mesh="co",
    primary_key=["order_id", "remittance_id"],  # Soporta múltiples PKs
    capacity="l",
    expody_name="expody_remittances"
)
```

### 2. Configurar Parámetros del Job

```python
from dynamodb_curated_library.core.config.job_parameters import JobParameters
from dynamodb_curated_library.core.config.job_config import JobConfig

# Parámetros de entrada
params = JobParameters(
    process_date="2024-01-15",      # FULL: YYYY-MM-DD
    # process_date="2024-01-15 14", # INC: YYYY-MM-DD HH
    account="123456789",
    env="pdn",
    process_type="FULL",  # o "INC"
    is_datalab=False
)

# Configuración del job
job_config = JobConfig(
    process_date=params.process_date,
    account=params.account,
    env=params.env,
    process_type=params.process_type,
    is_datalab=params.is_datalab,
    constants=constants,
    table_source=get_raw_dynamodb_source(params.process_type)
)
```

### 3. Extract: Leer Datos de S3

```python
from dynamodb_curated_library.etl.extract.catalog import Catalog
from dynamodb_curated_library.etl.extract.raw_sources import RawSources

# Con schema definido para FULL (evita OOM)
catalog = Catalog(
    glue_context=glue_context,
    job_config=job_config,
    schema=my_predefined_schema  # Opcional pero recomendado para FULL
)

sources = RawSources(catalog=catalog)
df_raw = sources.dynamodb_table
```

### 4. Transform: Aplanar y Transformar

```python
from dynamodb_curated_library.etl.transform.transformations import FlattenTransformations

# Flatten automático
flatten_transformations = FlattenTransformations(
    job_config=job_config,
    sources=sources
)
df_flatten, df_origin, record_count = flatten_transformations.get_flatten_table()

# Manejo de datos vacíos
if record_count == 0:
    logger.info("No records to process")
    return

# Transformaciones de negocio personalizadas
df_transform = apply_business_transformations(df_flatten, df_origin, job_config)
```

### 5. Metadata: Agregar Columnas de Auditoría

```python
from dynamodb_curated_library.metadata.metadata import MetadataConfig

metadata_config = MetadataConfig(
    file_name=job_config.table_name,
    module=constants.metadata_module,
    target_module="metadata"
)

df_final = metadata_config.add_metadata_columns(
    df=df_transform,
    job_config=job_config
)
```

### 6. Load: Guardar en Hudi

```python
from dynamodb_curated_library.etl.load.save import save_hudi

save_hudi(
    df=df_final,
    hudi_options=job_config.hudi_options,
    path=job_config.table_catalog_names.curated_table_path,
    mode=job_config.constants.insert_mode
)
```

## 🎨 Flatten Avanzado

### Control de Profundidad

```python
from dynamodb_curated_library.etl.transform.utils.flatten_dynamodb_struct import flatten_dynamodb_struct

# Limitar profundidad (estructuras más profundas → JSON)
df_flatten = flatten_dynamodb_struct(
    df,
    parent_col="Item",        # "Item" para FULL, "NewImage" para INC
    max_depth=3,              # Profundidad máxima de aplanado
    columns_as_json=["state.M.orderSteps"]  # Mantener como JSON
)
```

### Manejo de Errores

```python
from dynamodb_curated_library.etl.transform.utils.exceptions import FlattenStructureException

try:
    df_flatten = flatten_dynamodb_struct(df, parent_col="Item", max_depth=3)
except FlattenStructureException as e:
    # Excepción con sugerencias contextuales
    print(e)
    # Salida:
    # ================================================================================
    # ERROR: Failed to flatten DynamoDB structure
    # ================================================================================
    # SUGGESTIONS:
    #   1. Reduce max_depth parameter (current: 3)
    #   2. Add problematic columns to 'columns_as_json' parameter
    # Example:
    #   flatten_dynamodb_struct(df, max_depth=2, columns_as_json=['state.M.orderSteps'])
```

## 🔧 Alineación de Schema

Para mantener estabilidad en Glue Catalog:

```python
from dynamodb_curated_library.etl.transform.utils.align_schema import align_schema

# Alinear DataFrame al schema almacenado
df_aligned = align_schema(df_transform, stored_schema)

# Comportamiento:
# - Agrega columnas faltantes como NULL
# - Castea tipos a schema almacenado
# - Ignora columnas nuevas no esperadas
# - Mantiene orden del schema almacenado
```

## 🔄 Event Status Tracking (Opcional)

### Configuración

Habilita el tracking de operaciones DynamoDB (INSERT/UPDATE/DELETE):

```python
params = JobParameters(
    process_date="2024-01-15",
    account="123456789",
    env="pdn",
    process_type="FULL",
    event_status=True  # Habilita tracking
)
```

### Lógica de Operaciones

#### FULL Load
```python
# Todos los registros son INSERT (carga inicial)
flag_event_status = "INSERT"
```

#### INCREMENTAL Load
```python
# Compara con tabla existente en Glue Catalog:
# - Keys + NewImage + PK no existe → INSERT
# - Keys + NewImage + PK existe → UPDATE
# - Keys sin NewImage + PK existe → DELETE
```

### Configuración Hudi para Idempotencia

**Precombine Field Recomendado**: `job_process_date`

```python
hudi_options = {
    "hoodie.datasource.write.precombine.field": "job_process_date",
    "hoodie.datasource.write.operation": "upsert",
    # ... otras opciones
}
```

#### ¿Por qué `job_process_date` y no `update_date`?

**Problema con `update_date` (timestamp de DynamoDB)**:
```
FULL: Record A → update_date = "2026-01-01"
INC:  Record A → update_date = "2026-01-01" (MISMO VALOR)

Hudi precombine: "2026-01-01" == "2026-01-01"
→ Comportamiento INDEFINIDO
→ Puede perder datos del INC
```

**Ventaja de `job_process_date`**:
```
FULL: Record A → job_process_date = "2026-01-01"
INC:  Record A → job_process_date = "2026-01-01 02" (MÁS RECIENTE)

Hudi precombine: "2026-01-01 02" > "2026-01-01"
→ Hudi SIEMPRE toma el INC (más reciente)
→ Datos correctos garantizados
```

### Comportamiento en Reprocesamiento

**Escenario**: Reprocesar el mismo INC (ej: `2026-01-01 02`)

```
Primera ejecución:
- Record A: INSERT (job_process_date = "2026-01-01 02")

Reproceso del mismo INC:
- Record A: UPDATE (job_process_date = "2026-01-01 02")
```

**Resultado**:
- ✅ **Datos finales son CORRECTOS** (mismo contenido)
- ✅ **No hay pérdida de información**
- ⚠️ **flag_event_status puede cambiar** (INSERT → UPDATE)

**Conclusión**: Este comportamiento es **esperado y aceptable** porque:
1. Los datos de negocio siempre son correctos
2. `flag_event_status` es metadata informativa, no crítica
3. Hudi garantiza idempotencia con `job_process_date`

### Enriquecimiento de DELETE

Para registros DELETE, la librería recupera datos completos del catálogo:

```python
# DELETE solo tiene PKs en DynamoDB export
# La librería enriquece con datos del catálogo
df_enriched = enrich_deleted_records(
    df_with_status=df,
    catalog=catalog,
    job_config=job_config
)

# Resultado: Registro completo con flag_event_status = "DELETE"
```

## 📁 Estructura del Proyecto

```
dynamodb_curated_library/
├── core/
│   └── config/              # Configuración (Constants, JobConfig, JobParameters)
├── etl/
│   ├── extract/             # Lectura desde S3 (Catalog, Sources)
│   ├── transform/           # Transformaciones (Flatten, Curated)
│   │   └── utils/           # Utilidades (flatten_dynamodb_struct, align_schema)
│   └── load/                # Escritura Hudi (save, save_hudi)
├── metadata/                # Gestión de metadata (MetadataConfig)
├── models/                  # Modelos de datos (DynamoDB, Storage, Table)
├── dev_utils/               # Utilidades desarrollo (schema_formatter, local_env)
└── utils/                   # Utilidades generales (make_path)

examples/
└── co_delfos_productos_srf_transformar_remittances_order_status/
    ├── config/              # Configuración del ejemplo
    ├── schemas/             # Schemas predefinidos
    ├── transform/           # Transformaciones personalizadas
    ├── metadata/            # Metadata YAML
    └── *.py                 # Script principal

tests/
├── flatten_dynamodb/        # Tests unitarios de flatten
└── examples/                # Tests end-to-end de ejemplos
```

## 🧪 Testing

```bash
# Ejecutar todos los tests
pytest tests/ -v

# Tests de flatten
pytest tests/flatten_dynamodb/ -v

# Tests end-to-end
pytest tests/examples/remittances_order_status/ -v

# Con cobertura HTML
pytest tests/ --cov=dynamodb_curated_library --cov=examples --cov-report=html

# Ver reporte
open htmlcov/index.html
```

## 📊 Convenciones de Naming

### Buckets
```
{country_mesh}-{capacity}-{domain}-{raw|curated}-{account}-{env}
Ejemplo: co-l-productos-curated-123456789-pdn
```

### Database
```
{country_mesh}_{capacity}_{domain}_curated_{env}_rl
Ejemplo: co_delfos_productos_curated_pdn_rl
```

### Table
```
{country_mesh}_{prefix_name}_{data_product}
Ejemplo: co_dynamodb_remittances_order_status
```

### Columnas
Convertidas automáticamente a `snake_case`

## 🔍 Ejemplo Completo

Ver ejemplo completo en: `examples/co_delfos_productos_srf_transformar_remittances_order_status/`

## 📝 Documentación Adicional

- **Flatten**: `dynamodb_curated_library/etl/transform/utils/README.md`
- **Tests Flatten**: `tests/flatten_dynamodb/README.md`
- **Tests End-to-End**: `tests/examples/remittances_order_status/README.md`
- **Hudi**: `dynamodb_curated_library/core/config/hudi/README.md`

## 🤝 Contribución

1. Mantener cobertura de tests >= 85%
2. Documentar funciones en inglés
3. Usar `logger.debug()` para debugging (no `print()`)
4. Seguir convención `df_name` para DataFrames
5. Agregar tests para nuevas features

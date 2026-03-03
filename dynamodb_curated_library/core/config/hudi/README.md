# Hudi Configuration Guide

Guía completa de configuración de Apache Hudi para DynamoDB Curated Library.

## 📋 Tabla de Contenidos

1. [Configuración Base](#configuración-base)
2. [Configuraciones Personalizables](#configuraciones-personalizables)
3. [Casos de Uso Comunes](#casos-de-uso-comunes)
4. [Referencia Completa](#referencia-completa)

---

## Configuración Base

La librería proporciona una configuración base mínima que funciona para la mayoría de casos:

```python
{
    "hoodie.table.name": "nombre_tabla",
    "hoodie.datasource.write.recordkey.field": "primary_key",
    "hoodie.datasource.write.precombine.field": "momento_ingestion",
    "hoodie.metadata.enable": "false",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "database_name",
    "hoodie.datasource.hive_sync.table": "table_name",
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "mode": "append",
}
```

### Configuración de Particiones (Automática)

Si defines `partition_field` en Constants, se agregan automáticamente:

```python
{
    "hoodie.datasource.hive_sync.partition_fields": "year,month,day",
    "hoodie.datasource.write.partitionpath.field": "year,month,day",
}
```

---

## Configuraciones Personalizables

Usa `custom_hudi_options` en Constants para personalizar o agregar configuraciones:

### 1. **Storage Type** (Tipo de Almacenamiento)

```python
"hoodie.datasource.write.storage.type": "COPY_ON_WRITE"  # o "MERGE_ON_READ"
```

| Opción                | Descripción                                        | Cuándo Usar                                 |
| --------------------- | -------------------------------------------------- | ------------------------------------------- |
| `COPY_ON_WRITE` (COW) | Reescribe archivos completos en cada actualización | Lecturas frecuentes, escrituras ocasionales |
| `MERGE_ON_READ` (MOR) | Solo escribe deltas, merge en lectura              | Escrituras frecuentes, lecturas ocasionales |

**Recomendación**: COW para la mayoría de casos de DynamoDB curated.

---

### 2. **Write Operation** (Operación de Escritura)

```python
"hoodie.datasource.write.operation": "upsert"  # o "insert", "bulk_insert", "insert_overwrite"
```

| Operación          | Descripción                                  | Cuándo Usar                              |
| ------------------ | -------------------------------------------- | ---------------------------------------- |
| `upsert`           | Inserta o actualiza basado en primary key    | Procesos INCREMENTAL con actualizaciones |
| `insert`           | Solo inserta, falla si existe                | Datos únicos sin duplicados              |
| `bulk_insert`      | Inserta masivamente sin verificar duplicados | Carga inicial masiva (más rápido)        |
| `insert_overwrite` | Reemplaza particiones completas              | Procesos FULL que reemplazan todo        |
| `delete`           | Elimina registros                            | Soft deletes o limpieza                  |

**Recomendación**:
- `upsert` para procesos INCREMENTAL
- `insert_overwrite` para procesos FULL

---

### 3. **Metadata Table** (Tabla de Metadatos)

```python
"hoodie.metadata.enable": "true"  # o "false"
```

| Valor   | Ventajas                                            | Desventajas                          |
| ------- | --------------------------------------------------- | ------------------------------------ |
| `true`  | Mejora performance de queries y listado de archivos | Usa más espacio de almacenamiento    |
| `false` | Menos espacio                                       | Queries más lentos en tablas grandes |

**Recomendación**: `true` para producción, `false` si tienes restricciones de espacio.

---

### 4. **Hive Style Partitioning** (Particionado Estilo Hive)

```python
"hoodie.datasource.write.hive_style_partitioning": "true"  # o "false"
```

| Valor   | Formato de Path                                | Compatibilidad     |
| ------- | ---------------------------------------------- | ------------------ |
| `true`  | `s3://bucket/table/year=2024/month=01/day=15/` | Hive, Athena, Glue |
| `false` | `s3://bucket/table/2024/01/15/`                | Solo Hudi          |

**Recomendación**: `true` para compatibilidad con AWS Glue Catalog.

---

### 5. **Partition Extractor** (Extractor de Particiones)

```python
"hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor"
```

| Clase                                    | Cuándo Usar                              |
| ---------------------------------------- | ---------------------------------------- |
| `MultiPartKeysValueExtractor`            | Múltiples particiones (year, month, day) |
| `NonPartitionedExtractor`                | Tablas sin particiones                   |
| `SlashEncodedDayPartitionValueExtractor` | Particiones por día (YYYY/MM/DD)         |

**Recomendación**: `MultiPartKeysValueExtractor` si usas `partition_field`.

---

### 6. **Payload Class** (Manejo de Conflictos)

```python
"hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload"
```

| Clase                            | Comportamiento                                          |
| -------------------------------- | ------------------------------------------------------- |
| `DefaultHoodieRecordPayload`     | Usa `precombine_key` para decidir qué registro mantener |
| `OverwriteWithLatestAvroPayload` | Siempre sobrescribe con el último registro              |
| `EmptyHoodieRecordPayload`       | Para operaciones de delete                              |

**Recomendación**: Default es suficiente para la mayoría de casos.

---

### 7. **Sync Comments** (Sincronizar Comentarios)

```python
"hoodie.datasource.hive_sync.sync_comment": "true"  # o "false"
```

| Valor   | Efecto                                             |
| ------- | -------------------------------------------------- |
| `true`  | Sincroniza comentarios de columnas al Glue Catalog |
| `false` | No sincroniza comentarios                          |

**Recomendación**: `true` para mejor documentación en el catálogo.

---

### 8. **Schema Evolution** (Evolución de Schema)

```python
"hoodie.datasource.write.schema.allow.auto.evolution": "true"  # o "false"
```

| Valor   | Efecto                                   |
| ------- | ---------------------------------------- |
| `true`  | Permite agregar columnas automáticamente |
| `false` | Falla si el schema cambia                |

**Recomendación**: `false` para estabilidad, usa `align_schema()` para control manual.

---

### 9. **Null Handling** (Manejo de Nulos)

```python
"hoodie.write.set.null.for.missing.columns": "true"  # o "false"
```

| Valor   | Efecto                                |
| ------- | ------------------------------------- |
| `true`  | Columnas faltantes se llenan con NULL |
| `false` | Falla si faltan columnas              |

**Recomendación**: `true` para flexibilidad con schemas variables.

---

### 10. **Cleaning Configuration** (Limpieza Automática)

```python
{
    "hoodie.clean.automatic": "true",
    "hoodie.clean.async": "true",
    "hoodie.cleaner.policy": "KEEP_LATEST_FILE_VERSIONS",
    "hoodie.cleaner.fileversions.retained": "1",
    "hoodie.clean.trigger.strategy": "NUM_COMMITS",
    "hoodie.clean.max.commits": "1",
}
```

| Opción                                 | Descripción                  | Valor Recomendado           |
| -------------------------------------- | ---------------------------- | --------------------------- |
| `hoodie.clean.automatic`               | Habilita limpieza automática | `true`                      |
| `hoodie.clean.async`                   | Limpieza asíncrona           | `true`                      |
| `hoodie.cleaner.policy`                | Política de limpieza         | `KEEP_LATEST_FILE_VERSIONS` |
| `hoodie.cleaner.fileversions.retained` | Versiones a mantener         | `1` (solo última)           |
| `hoodie.clean.trigger.strategy`        | Cuándo limpiar               | `NUM_COMMITS`               |
| `hoodie.clean.max.commits`             | Limpiar cada N commits       | `1` (cada commit)           |

**Recomendación**: Usar para procesos FULL que reemplazan datos completos.

---

## Casos de Uso Comunes

### Caso 1: Proceso INCREMENTAL con Particiones

```python
_CUSTOM_HUDI_OPTIONS = {
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.metadata.enable": "true",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
    "hoodie.datasource.hive_sync.sync_comment": "true",
}
```

---

### Caso 2: Proceso FULL sin Particiones

```python
_CUSTOM_HUDI_OPTIONS = {
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "insert_overwrite",
    "hoodie.metadata.enable": "false",
    "hoodie.datasource.write.hive_style_partitioning": "false",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.NonPartitionedExtractor",
    "hoodie.write.set.null.for.missing.columns": "true",
    "hoodie.datasource.write.schema.allow.auto.evolution": "true",
    # Limpieza automática
    "hoodie.clean.automatic": "true",
    "hoodie.clean.async": "true",
    "hoodie.cleaner.policy": "KEEP_LATEST_FILE_VERSIONS",
    "hoodie.cleaner.fileversions.retained": "1",
    "hoodie.clean.trigger.strategy": "NUM_COMMITS",
    "hoodie.clean.max.commits": "1",
}
```

---

### Caso 3: Carga Inicial Masiva (Bulk Insert)

```python
_CUSTOM_HUDI_OPTIONS = {
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "bulk_insert",
    "hoodie.metadata.enable": "false",  # Deshabilitar para velocidad
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.bulkinsert.shuffle.parallelism": "200",  # Paralelismo alto
}
```

---

## Referencia Completa

### Configuración en Constants

```python
from dynamodb_curated_library.core.config.constants import Constants

_CUSTOM_HUDI_OPTIONS = {
    # Tus configuraciones personalizadas aquí
    "hoodie.datasource.write.operation": "upsert",
    # Para remover una configuración base, usa None
    "hoodie.datasource.write.precombine.field": None,
}

constants = Constants.build_constants({
    "domain": "productos",
    "subdomain": "transacciones",
    "data_product": "remittances",
    "country_mesh": "co",
    "primary_key": ["order_id", "remittance_id"],
    "precombine_key": "momento_ingestion",
    "partition_field": "creation_date",  # Opcional
    "custom_hudi_options": _CUSTOM_HUDI_OPTIONS,  # Opcional
})
```

### Remover Configuraciones Base

Para remover una configuración de la base, usa `None`:

```python
_CUSTOM_HUDI_OPTIONS = {
    "hoodie.datasource.write.precombine.field": None,  # Remueve precombine
}
```

---

## 📚 Referencias

- [Apache Hudi Documentation](https://hudi.apache.org/docs/configurations)
- [AWS Glue Hudi Connector](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html)
- [Hudi Configuration Properties](https://hudi.apache.org/docs/configurations#Write-Options)

---

## 🤝 Contribución

Para agregar nuevas configuraciones o casos de uso, actualiza este documento y `hudi_config_builder.py`.

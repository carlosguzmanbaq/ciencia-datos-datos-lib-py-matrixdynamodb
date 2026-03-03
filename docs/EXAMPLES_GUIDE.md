# Guía de Ejecución de Ejemplos

Esta guía explica cómo ejecutar los ejemplos del proyecto localmente.

---

## 📋 Prerequisitos

### 1. Instalar Dependencias

```bash
# Con Poetry (recomendado)
poetry install -E spark

# Activar entorno
source $(poetry env info --path)/bin/activate
```

### 2. Configurar AWS Credentials

```bash
aws configure
# O exportar variables
export AWS_ACCESS_KEY_ID=<tu-access-key>
export AWS_SECRET_ACCESS_KEY=<tu-secret-key>
```

---

## 🚀 Ejecución Rápida

### Opción 1: Usando run.sh

```bash
# 1. Activar entorno
source $(poetry env info --path)/bin/activate

# 2. Ejecutar script
./run.sh
```

### Opción 2: Comando Directo

```bash
export LOCAL_ENV="DEV"

python -m examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.co_delfos_productos_srf_transformar_surprise_gift_redeems \
  --JOB_NAME=local \
  --PROCESS_DATE="2026-01-18 06" \
  --ACCOUNT=710976466405 \
  --ENV=pdn \
  --PROCESS_TYPE=INC
```

---

## 📝 Parámetros de Ejecución

### Obligatorios

| Parámetro        | Descripción            | Ejemplo                                         |
| ---------------- | ---------------------- | ----------------------------------------------- |
| `--JOB_NAME`     | Nombre del job         | `local`                                         |
| `--PROCESS_DATE` | Fecha de procesamiento | `"2026-01-18"` (FULL) o `"2026-01-18 06"` (INC) |
| `--ACCOUNT`      | ID cuenta AWS          | `710976466405`                                  |
| `--ENV`          | Ambiente               | `pdn`, `dev`, `qa`                              |
| `--PROCESS_TYPE` | Tipo de carga          | `FULL` o `INC`                                  |

### Opcionales

| Parámetro        | Descripción      | Default | Ejemplo |
| ---------------- | ---------------- | ------- | ------- |
| `--IS_DATALAB`   | Ambiente Datalab | `False` | `True`  |
| `--LOG_LEVEL`    | Nivel de logging | `INFO`  | `DEBUG` |
| `--EVENT_STATUS` | Tracking CDC     | `False` | `True`  |

---

## 📚 Ejemplos Disponibles

### 1. Remittances Order Status

#### FULL Load - Mesh
```bash
export LOCAL_ENV="DEV"

python -m examples.co_delfos_productos_srf_transformar_remittances_order_status.co_delfos_productos_srf_transformar_remittances_order_status \
  --JOB_NAME=local \
  --PROCESS_DATE="2025-09-22" \
  --ACCOUNT=710976466405 \
  --ENV=pdn \
  --PROCESS_TYPE=FULL \
  --LOG_LEVEL=DEBUG
```

#### INCREMENTAL Load - Mesh
```bash
export LOCAL_ENV="DEV"

python -m examples.co_delfos_productos_srf_transformar_remittances_order_status.co_delfos_productos_srf_transformar_remittances_order_status \
  --JOB_NAME=local \
  --PROCESS_DATE="2025-09-22 01" \
  --ACCOUNT=710976466405 \
  --ENV=pdn \
  --PROCESS_TYPE=INC \
  --EVENT_STATUS=True
```

---

### 2. Surprise Gift Redeems

#### FULL Load - Mesh
```bash
export LOCAL_ENV="DEV"

python -m examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.co_delfos_productos_srf_transformar_surprise_gift_redeems \
  --JOB_NAME=local \
  --PROCESS_DATE="2026-01-17" \
  --ACCOUNT=710976466405 \
  --ENV=pdn \
  --PROCESS_TYPE=FULL
```

#### INCREMENTAL Load - Mesh
```bash
export LOCAL_ENV="DEV"

python -m examples.co_delfos_productos_srf_transformar_surprise_gift_redeems.co_delfos_productos_srf_transformar_surprise_gift_redeems \
  --JOB_NAME=local \
  --PROCESS_DATE="2026-01-18 06" \
  --ACCOUNT=710976466405 \
  --ENV=pdn \
  --PROCESS_TYPE=INC
```

---

### 3. Customer Know ID Module

#### FULL Load - Mesh
```bash
export LOCAL_ENV="DEV"

python -m examples.co_delfos_clientes_prn_transformar_customerknowid_module.co_delfos_clientes_prn_transformar_customerknowid_module \
  --JOB_NAME=local \
  --PROCESS_DATE="2026-01-06" \
  --ACCOUNT=158304509201 \
  --ENV=pdn \
  --PROCESS_TYPE=FULL \
  --EVENT_STATUS=True
```

#### INCREMENTAL Load - Mesh
```bash
export LOCAL_ENV="DEV"

python -m examples.co_delfos_clientes_prn_transformar_customerknowid_module.co_delfos_clientes_prn_transformar_customerknowid_module \
  --JOB_NAME=local \
  --PROCESS_DATE="2026-01-16 06" \
  --ACCOUNT=158304509201 \
  --ENV=pdn \
  --PROCESS_TYPE=INC \
  --EVENT_STATUS=True
```

---

## 🔧 Troubleshooting

### Error: No module named 'dynamodb_curated_library'

```bash
source $(poetry env info --path)/bin/activate
poetry install -E spark
```

### Error: RecursionError (Python 3.14)

```bash
poetry env remove python3.14
poetry env use python3.10
poetry install -E spark
```

### Error: Invalid PROCESS_DATE format

```bash
# FULL: YYYY-MM-DD
--PROCESS_DATE="2026-01-17"

# INC: YYYY-MM-DD HH
--PROCESS_DATE="2026-01-18 06"
```

---

## 📊 Logs de Ejemplo

```
INFO - Starting job: surprise_gift_redeems
INFO - Process Type: INC
INFO - Process Date: 2026-01-18 06
INFO - Records found: 1,234
INFO - Flatten completed: 1,234 records
INFO - Data saved successfully
INFO - Job completed!
```

---

## 📖 Referencias

- [README.md](../README.md)
- [POETRY_GUIDE](https://python-poetry.org/docs/)


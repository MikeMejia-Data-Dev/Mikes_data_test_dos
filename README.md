# DataMart S.A.S. — Pipeline ETL con Apache Airflow

Pipeline ETL de datos de retail construido con Apache Airflow 3.2, PostgreSQL externo y Docker. Procesa ~1.5M de transacciones de dos fuentes históricas, las transforma, deduplica y carga en un datawarehouse analítico estrella.

---

## Arquitectura

```
data/raw/
  ├── data.csv                  # Fuente diaria (~541,909 filas, latin-1)
  └── online_retail_II.xlsx     # Historial extendido (~1,067,371 filas, 2 hojas)
        ↓
  [Airflow DAG: datamart_retail_pipeline]
        ↓
  PostgreSQL externo (15.204.173.204:6432)
  ├── mike_dos_airflow    → metadata de Airflow
  └── mike_dos_dw         → datawarehouse analítico
```

### Modelo de datos (estrella)

```
dim_product ──┐
              ├── fact_transactions
dim_date    ──┘

rejected_records   (registros inválidos)
vw_net_revenue_by_product_day  (vista analítica)
```

---

## Requisitos

- Docker y Docker Compose
- Acceso al servidor PostgreSQL externo (`15.204.173.204:6432`)
- Credenciales de base de datos (solicitar al administrador)

---

## Instalación en menos de 10 minutos

### 1. Clonar el repositorio

```bash
git clone <repo-url>
cd datamart
```

### 2. Crear el archivo `.env`

Copiar el ejemplo y completar con las credenciales reales:

```bash
cp .env.example .env
nano .env
```

Contenido del `.env`:

```env
# Base de datos externa
EXTERNAL_DB_HOST=15.204.173.204
EXTERNAL_DB_PORT=6432

# Airflow metadata DB
AIRFLOW_DB_NAME=mike_dos_airflow
AIRFLOW_DB_USER=<usuario_airflow>
AIRFLOW_DB_PASSWORD=<password_airflow>

# Datawarehouse analítico
DW_DB_NAME=mike_dos_dw
DW_DB_USER=<usuario_dw>
DW_DB_PASSWORD=<password_dw>

# Airflow admin
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=<password_admin>

# Seguridad
FERNET_KEY=<generar con: python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())">
JWT_SECRET=<generar con: python3 -c "import secrets; print(secrets.token_hex(64))">
```

### 3. Colocar los archivos de datos

```bash
mkdir -p data/raw
# Copiar data.csv y online_retail_II.xlsx a data/raw/
```

### 4. Fijar permisos

```bash
mkdir -p logs data/intermediate
sudo chown -R 50000:0 logs data/intermediate
```

### 5. Levantar Docker

```bash
docker compose up --build
```

El servicio `airflow-init` correrá automáticamente y:
- Aplicará el esquema SQL al datawarehouse
- Migrará la base de metadata de Airflow
- Creará la conexión `postgres_dw`
- Creará las variables de Airflow

### 6. Acceder a la UI

```
http://localhost:8080
Usuario: admin
Password: (se genera automáticamente — ver logs del webserver)
```

```bash
docker compose logs airflow-webserver | grep "Password for user"
```

---

## Estructura del repositorio

```
datamart/
├── dags/
│   └── retail_pipeline_dag.py      # DAG principal
├── scripts/
│   ├── database.py                 # Conexión vía PostgresHook
│   ├── extract.py                  # Extracción de fuentes
│   ├── transform.py                # Limpieza, clasificación, dedup
│   ├── load.py                     # Carga idempotente al DW
│   └── io_utils.py                 # Lectura/escritura de archivos intermedios (parquet)
├── sql/
│   └── create_tables.sql           # DDL completo del modelo estrella
├── data/
│   ├── raw/                        # Archivos fuente (NO incluidos en git)
│   └── intermediate/               # Parquets temporales entre tasks
├── logs/                           # Logs de Airflow (NO incluidos en git)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## DAG: `datamart_retail_pipeline`

**Schedule:** `@daily`

**Flujo de tasks:**

```
extract_daily_sales ──────┐
                          ├──► transform_and_dedup ──► load_dimensions ──► assign_date_id ──► load_facts_and_rejected
extract_historical_trans ─┘
```

| Task | Descripción |
|------|-------------|
| `extract_daily_sales` | Lee `data.csv` (latin-1, ~541K filas) |
| `extract_historical_transactions` | Lee `online_retail_II.xlsx` (2 hojas, ~1.06M filas) |
| `transform_and_dedup` | Limpieza, normalización, dedup entre fuentes (~65K duplicados eliminados) |
| `load_dimensions` | Carga `dim_product` y `dim_date` (idempotente vía UPSERT) |
| `assign_date_id` | Asigna FK de fecha a cada transacción |
| `load_facts_and_rejected` | Carga `fact_transactions` y `rejected_records` (DELETE+INSERT por source_file) |

---

## Reglas de negocio

| Regla | Descripción |
|-------|-------------|
| Devoluciones | `quantity <= 0` → `transaction_type = 'return'` (se incluyen en el análisis) |
| Rechazos | `unit_price <= 0` → siempre rechazado a `rejected_records` |
| Fechas | Se asumen en `Europe/London`, se convierten a UTC |
| `product_code` | Normalizado a mayúsculas sin espacios |
| `customer_id` | Sin ID → `'UNKNOWN'`, `is_identified = False` (se incluyen en análisis) |
| Nombre canónico | Descripción más frecuente (moda) por `product_code` |
| Categorías | Heurística por palabras clave → 5 categorías: Electrónica, Hogar, Ropa, Deportes, Papelería |
| Dedup | Clave: `(invoice_no, product_code, customer_id, transaction_date_utc)` — prioridad a `data_csv` |
| Revenue | `gross_revenue = quantity * unit_price` (con signo, preserva dirección de devolución) |

---

## Idempotencia

El pipeline es seguro de reejecutar. Ejecutar el DAG dos veces con los mismos datos produce el mismo resultado:

- `fact_transactions` y `rejected_records`: DELETE por `source_file` antes de INSERT
- `dim_product`: UPSERT con `ON CONFLICT DO UPDATE`
- `dim_date`: `ON CONFLICT DO NOTHING`

---

## Decisiones técnicas

### XCom vs archivos intermedios

Se optó por escribir DataFrames como parquet en disco compartido (`data/intermediate/`) en lugar de usar XCom, dado el volumen real de ~1.6M filas combinadas. XCom solo pasa la ruta del archivo.

### Bases de datos externas

Las bases de datos **no corren dentro de Docker**. Existe un único servidor PostgreSQL externo que alberga tanto la metadata de Airflow como el datawarehouse analítico. Esto simplifica la infraestructura y permite persistencia independiente del ciclo de vida de los contenedores.

### Fuente `online_retail_II`

El dataset preferido es el CSV de Kaggle (`thedevastator/online-retail-transaction-dataset`). Si no existe, el código hace fallback automático al `.xlsx` con dos hojas (`Year 2009-2010` y `Year 2010-2011`). No es una fuente de devoluciones separada — las devoluciones se detectan por `quantity <= 0` dentro de cada fuente.

### Categorización sin API

La asignación de categoría usa una heurística de palabras clave sobre `description`. No requiere API externa. Mapa a 5 categorías con `Hogar` como default.

---

## Recortes documentados

Los siguientes elementos fueron excluidos por restricción de tiempo:

- API REST de catálogo de productos (plus opcional)
- Diagrama formal en dbdiagram.io (DDL disponible en `sql/create_tables.sql`)
- Tests exhaustivos de calidad de datos

---

## Validación previa al despliegue

Resultados validados localmente con datos reales antes del despliegue Docker:

| Métrica | Valor |
|---------|-------|
| Filas extraídas (daily) | 541,909 |
| Filas extraídas (historical) | 1,067,371 |
| Filas válidas tras transformación | 1,600,556 |
| Duplicados eliminados | 65,485 |
| Filas finales en fact_transactions | 1,535,071 |
| Registros rechazados | 8,724 |
| Productos únicos | 4,760 |

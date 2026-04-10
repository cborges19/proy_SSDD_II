# Pipeline de Datos de Airbnb Málaga - Airflow (Local) y Kafka (Docker)

Este proyecto implementa una arquitectura ETL completa para el procesamiento, limpieza, enriquecimiento y análisis de datos de alojamiento turístico en Málaga, cumpliendo con los estándares requeridos en la Práctica 1 (SDPD2-GCID).

La orquestación se realiza mediante Apache Airflow (ejecutado localmente mediante `uv`), y los datos finales son serializados en formato Avro y enviados a un clúster de Apache Kafka (virtualizado con Docker) a través de un Schema Registry.

## Estructura del Proyecto

```text
.
├── config.toml                     # Configuración centralizada (rutas y red)
├── docker-compose.yml              # Infraestructura de streaming (Kafka, Zookeeper, SR)
├── pyproject.toml / uv.lock        # Gestión estricta de dependencias
├── data/                           
│   ├── raw/                        # CSVs originales (listings, calendar, reviews)
│   └── output/                     # Resultados y reportes (HTML) generados
└── src/                            # Módulos y lógica de negocio
    ├── __init__.py                 # Obligatorio para reconocimiento de módulos
    ├── utils.py                    # Funciones modulares de limpieza, imputación y Kafka
    ├── transformations/            # DAGs de Airflow
    │   ├── dag_listings.py
    │   ├── dag_calendar.py
    │   └── dag_reviews.py
    └── reports/                    # Lógica de EDA y plantillas web
        ├── __init__.py
        ├── report_listings.py      
        ├── report_calendar.py
        ├── report_reviews.py
        ├── report_listings.html    # Plantillas Jinja2 base
        ├── report_calendar.html
        └── report_reviews.html
```

## Guía de Reproducción (Windows / Ubuntu / Mac)

La arquitectura híbrida requiere tener Docker para el streaming de datos y el gestor de paquetes `uv` para ejecutar Airflow localmente. 

### Prerrequisitos
- Docker instalado y ejecutándose.
- `uv` instalado (`pip install uv`).
- Python 3.10 o superior instalado en el sistema.
- Asegúrese de que en `config.toml` los puertos de Kafka apunten a `localhost` (ej. `localhost:9092`) ya que Airflow se ejecuta fuera de la red de Docker.

### Paso 0: Preparación de los Datos
Por buenas prácticas de control de versiones, los datos originales no se incluyen en el repositorio de GitHub. Antes de iniciar la ejecución, debe descargar los datos de origen:
1. Descargue el dataset de **Inside Airbnb** correspondiente a la ciudad de Málaga (archivos `listings.csv`, `calendar.csv` y `reviews.csv`).
2. Mueva estos tres archivos al directorio `data/raw/` dentro del proyecto. 

*(Nota: Aunque la carpeta esté vacía al clonar el repositorio gracias al archivo `.gitkeep`, es estrictamente necesario que los archivos CSV estén allí alojados para que los pipelines funcionen correctamente).*

### Paso 1: Inicializar la infraestructura de Kafka
Abra una terminal en la raíz del proyecto y levante el clúster de streaming:
```bash
docker compose up -d
```

### Paso 2: Instalación de dependencias
En lugar de instalar Airflow globalmente, el proyecto utiliza `pyproject.toml` y `uv.lock`. Ejecute el siguiente comando para que `uv` cree un entorno virtual (`.venv`) e instale exactamente las versiones correctas de Apache Airflow, Pandas, Kafka y el resto de librerías necesarias:

```bash
uv sync
```
Este paso prepara el entorno sin necesidad de instalar dependencias adicionales manualmente.

### Paso 3: Configuración del entorno local de Airflow y Python
Para mantener el proyecto autocontenido y asegurar que Airflow encuentre los módulos personalizados y los DAGs, es necesario configurar las variables de entorno. Dependiendo de su terminal, ejecute:

**Para Ubuntu / Linux / Mac (Bash/Zsh):**
```bash
export AIRFLOW_HOME=$(pwd)
export PYTHONPATH=$(pwd)
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/src/transformations
```

**Para Windows (PowerShell):**
```powershell
$env:AIRFLOW_HOME=$PWD
$env:PYTHONPATH=$PWD
$env:AIRFLOW__CORE__DAGS_FOLDER="$PWD\src\transformations"
```

**Para Windows (CMD):**
```cmd
set AIRFLOW_HOME=%cd%
set PYTHONPATH=%cd%
set AIRFLOW__CORE__DAGS_FOLDER=%cd%\src\transformations
```

### Paso 4: Ejecución de Airflow
Ejecute el siguiente comando para iniciar la base de datos, el planificador (scheduler) y el servidor web simultáneamente:

```bash
uv run airflow standalone
```

Este comando realiza automáticamente las siguientes acciones:
* Inicializa y migra la base de datos SQLite local.
* Crea un usuario administrador por defecto (`admin`).
* Inicia el planificador y el servidor web en el puerto 8080.

*Nota: Durante este proceso, Airflow generará una contraseña segura aleatoria que se imprimirá en la terminal. Esta credencial también quedará guardada en un archivo llamado `standalone_admin_password.txt` en la raíz de su proyecto.*

### Paso 5: Ejecución de los Pipelines
1. Abra su navegador web y diríjase a: `http://localhost:8080`
2. Inicie sesión con el usuario `admin` y la contraseña generada en el paso anterior.
3. En la interfaz visualizará los 3 pipelines desarrollados (`airbnb_master_pipeline`, `airbnb_calendar_pipeline`, `airbnb_reviews_pipeline`).
4. Active los DAGs deslizando el interruptor situado a su izquierda (Unpause).
5. Haga clic en el botón de ejecución (Trigger DAG) bajo la columna *Actions* para iniciar el procesamiento.

### Resultados Generados
Tras la ejecución exitosa de los DAGs:
1. **Archivos Intermedios:** Se almacenarán temporalmente los archivos `.parquet` procesados para optimizar el rendimiento.
2. **Reportes Visuales:** En el directorio `./data/output/reports/` se generarán tres dashboards interactivos en formato `.html` con las visualizaciones del Análisis Exploratorio de Datos (EDA).
3. **Kafka:** Los datos enriquecidos se publicarán automáticamente serializados en formato Avro en los tópicos del contenedor Docker (`airbnb_listings_gold`, `airbnb_calendar_gold`, `airbnb_reviews_gold`), listos para su posterior consumo.

## Verificación de Datos en Kafka

Para demostrar que los datos se están procesando y enviando correctamente a las capas "Gold" en el formato **Apache Avro**, se ha incluido un script de utilidad que permite inspeccionar los mensajes en tiempo real.

### Requisitos previos
* Tener los contenedores de Docker en ejecución (`docker-compose up`).
* Tener instalada la herramienta `jq` en tu sistema (opcional, para ver el JSON con colores).

### Uso del Script de Verificación
El script `check_kafka.sh` es generalizable y permite visualizar cualquier topic del sistema:

1. **Dar permisos de ejecución (solo la primera vez):**
   ```bash
   chmod +x check_kafka.sh
   ```

2. **Ver datos de Listings (Capa Gold - Formato Avro):**
   Muestra el último listado procesado con todas las variables de enriquecimiento (distancia al centro, trust score, etc.):
   ```bash
   ./check_kafka.sh airbnb_listings_gold 1
   ```

3. **Ver datos de Reviews (Capa Gold - Formato Avro):**
   Verifica que los comentarios de las reviews han sido limpiados mediante NLP:
   ```bash
   ./check_kafka.sh airbnb_reviews_gold 1
   ```

4. **Ver datos de Calendar (Capa Gold - Formato Avro):**
   Verifica que las series temporales del calendario han sido preprocesadas:
   ```bash
   ./check_kafka.sh airbnb_calendar_gold 1
   ```

5. **Ver errores de Validación (Dead Letter Queue):**
   Si alguna regla de calidad de datos falló, los detalles aparecerán aquí en formato JSON:
   ```bash
   ./check_kafka.sh pipeline_errors 5
   ```
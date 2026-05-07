# Pipeline de Datos de Airbnb Málaga - Airflow (Local) y Kafka (Docker)

Este proyecto implementa una arquitectura ETL completa para el procesamiento, limpieza, enriquecimiento y análisis de datos de alojamiento turístico en Málaga, cumpliendo con los estándares requeridos en la Práctica 1 (SDPD2-GCID).

La orquestación se realiza mediante Apache Airflow (ejecutado localmente mediante `uv`), y los datos finales son serializados en formato Avro y enviados a un clúster de Apache Kafka (virtualizado con Docker) a través de un Schema Registry.

## Estructura del Proyecto

```text
.
├── config.toml                     # Configuración general (puertos de Kafka, datos...)
├── docker-compose.yml              # Infraestructura de streaming (Kafka)
├── pyproject.toml / uv.lock        # Gestión estricta de dependencias
├── README.md                       # Documentación del proyecto
├── check_kafka.sh                  # Script de validación de Kafka
├── data/                           
│   ├── raw/                        # CSVs originales (+ neighbourhoods.csv/geojson)
│   └── output/                     # Directorio de resultados
├── notebooks/                      # Entornos de exploración y prueba
│   ├── eda/                        # Análisis exploratorio (incluye subcarpeta listings/)
│   ├── prototyping/                # Prototipado en sucio de los DAGs
│   └── queries/                    # Consultas interactivas (incluye el mapa en vivo)
│       ├── calendar_query.ipynb
│       ├── map_reviews_query.ipynb
│       └── reviews_query.ipynb
└── src/                            # Módulos y lógica de negocio
    ├── __init__.py                 
    ├── utils.py                    
    ├── kafka/                      # Integración directa con Kafka
    │   ├── consumer_kafka.py
    │   └── producer_kafka.py
    ├── queries/                    # Scripts productivizados de Spark Streaming
    │   ├── calendar_query.py
    │   ├── reviews_query.py
    │   └── error_counter.py
    ├── transformations/            # DAGs orquestados por Airflow
    │   ├── dag_listings.py
    │   ├── dag_calendar.py
    │   └── dag_reviews.py
    └── reports/                    # Lógica de EDA y plantillas web
        ├── report_listings.py      
        ├── report_calendar.py
        ├── report_reviews.py
        ├── report_listings.html    
        ├── report_calendar.html
        └── report_reviews.html
```

## Parte 1: Guía de Reproducción de la Infraestructura y ETL (Windows / Ubuntu / Mac)

La arquitectura híbrida requiere tener Docker para el streaming de datos y el gestor de paquetes `uv` para ejecutar Airflow localmente. 

### Prerrequisitos
- Docker instalado y ejecutándose.
- `uv` instalado (`pip install uv`).
- Python 3.10 o superior instalado en el sistema.
- Asegúrese de que en `config.toml` los puertos de Kafka apunten a `localhost` (ej. `localhost:9092`) ya que Airflow se ejecuta fuera de la red de Docker.

### Paso 0: Preparación de los Datos
Por buenas prácticas de control de versiones, los datos originales no se incluyen en el repositorio de GitHub. Antes de iniciar la ejecución, debe descargar los datos de origen:
1. Descargue el dataset de **Inside Airbnb** correspondiente a la ciudad de Málaga (archivos `listings.csv.gz`, `calendar.csv.gz` y `reviews.csv.gz`) en el siguiente enlace: https://insideairbnb.com/get-the-data/.
2. Descomprima estos archivos.
3. Mueva los ficheros csv al directorio `data/raw/` dentro del proyecto. 

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

## Paso Opcional :Verificación de Datos en Kafka

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
   
## Parte 2: Procesamiento con Spark Streaming y Entregables

Esta sección detalla cómo ejecutar la segunda parte del proyecto, centrada en el consumo de datos y resolución de consultas continuas mediante PySpark. 

**Nota de ejecución:** Los siguientes comandos utilizan sintaxis general y estándar de Python. Es importante estar en la raíz del proyecto en la terminal y activar el entorno virtual según su sistema operativo:

**Para Ubuntu / Linux / Mac:**
```bash
source .venv/bin/activate
```

**Para Windows (PowerShell):**
```powershell
.venv\Scripts\Activate.ps1
```

**Para Windows (CMD):**
```cmd
.venv\Scripts\activate.bat
```

### Paso 6: Consumo manual de Apache Kafka
Para verificar los mensajes directamente desde Kafka (sin Spark) y visualizar el esquema inferido, ejecute el script del consumidor. Este actuará como monitor en tiempo real. Los nombres de los topics son `airbnb_listings_gold`, `airbnb_reviews_gold`, `airbnb_calendar_gold` o `pipeline_errors`.

**Para Ubuntu / Linux / Mac:**
```bash
python3 src/kafka/consumer_kafka.py [topic]
```

**Para Windows (PowerShell / CMD):**
```bash
python src\kafka\consumer_kafka.py [topic]
```

*Si se ejecutan los DAGs de Airflow en paralelo, se visualizará por consola la recepción de mensajes en el tópico `airbnb_listings_gold` por defecto.*

### Paso 7: Ejecución de Consultas con Spark Streaming (Avro)
Los scripts de consulta ya están preconfigurados para descargar dinámicamente las dependencias necesarias de Kafka y Avro para Spark (`org.apache.spark:spark-sql-kafka-0-10`, `org.apache.spark:spark-avro`). 

> **Nota técnica sobre la generación de volcados:**
> Para almacenar los resultados, se utiliza el operador de redirección del sistema operativo (`>`) apuntando a la carpeta `data/output/`. Se ha optado por esta estrategia en lugar de usar un sumidero de archivos nativo de Spark (`.format("text")`) por dos motivos arquitectónicos:
> 1. Spark Structured Streaming **no soporta el modo `complete`** (necesario para ver tablas de agregaciones globales actualizadas) en sumideros de tipo archivo.
> 2. Spark no genera un único archivo `.txt` limpio, sino un directorio particionado con metadatos. La redirección de la salida estándar mantiene el código limpio y genera exactamente el entregable requerido.

**Consulta 1: Análisis del Calendario**
Calcula la ocupación media y las reservas totales mediante ventanas temporales:

* **Ubuntu / Linux / Mac:** `python3 src/queries/calendar_query.py > data/output/salida_1.txt`
* **Windows:** `python src\queries\calendar_query.py > data\output\salida_1.txt`

**Consulta 2: Análisis de Sentimiento en Reviews**
Lee en streaming los comentarios, aplica un modelo NLP (VADER) implementado mediante `pandas_udf` y cuenta el tipo de review en ventanas anuales:

* **Ubuntu / Linux / Mac:** `python3 src/queries/reviews_query.py > data/output/salida_2.txt`
* **Windows:** `python src\queries\reviews_query.py > data\output\salida_2.txt`

**Consulta 3: Revisión de Errores**
Esta es una consulta extra que lee información básica sobre errores en validación del DAG de los datos.

* **Ubuntu / Linux / Mac:** `python3 src/queries/error_counter.py > data/output/salida_3.txt`
* **Windows:** `python src\queries\error_counter.py > data\output\salida_3.txt`

**Consulta 4: Análisis de Sentimiento en Mapa**
Lee dos streaming de datos (listings y reviews), haciendo un join y graficando un mapa por coordenadas con la ubicación de alojamientos con reviews buenas y malas.

En el caso de esta consulta, para ver el resultado hay que ejecutar el notebook interactivo en `notebooks/queries/map_reviews_query.ipynb`.

### Entregables de la Práctica
Para facilitar la evaluación, los artefactos solicitados se encuentran estructurados de la siguiente manera:

1.  **Capturas de pantalla del Producer y Consumer (sin Spark):** 
    *   El *Producer* se evidencia en los logs de éxito de las tareas finales de los DAGs en Airflow.
    *   El *Consumer* manual se visualiza ejecutando el Paso 6. Las capturas están adjuntas en el documento de entrega.
2.  **Resolución de dependencias Spark Streaming (Avro):**
    *   Evidenciado al ejecutar cualquiera de los scripts de la carpeta `src/queries/`. Al inicializar la sesión, Spark descargará los `.jar` necesarios especificados en la configuración del builder. Las capturas del log de descargas están en el documento de entrega.
3.  **Captura de las tablas leídas:**
    *   La visualización del streaming escribiendo en consola (`format("console")`) se muestra automáticamente por consola al final de la ejecución de las consultas.
4.  **Respuestas a las preguntas:**
    *   Incluidas en la memoria/documento PDF adjunto.
5.  **Scripts con consultas realizadas:**
    *   Ubicados en `src/queries/calendar_query.py`, `src/queries/reviews_query.py` y `src/queries/error_counter.py`.
```
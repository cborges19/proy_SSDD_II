#!/bin/bash

# --- DETECCIÓN DE CONTENEDORES ---
SR_CONTAINER=$(docker ps --filter "ancestor=confluentinc/cp-schema-registry:7.4.0" --format "{{.Names}}")

if [ -z "$1" ]; then
    echo "Uso: ./check_kafka.sh [nombre_del_topic] [numero_de_mensajes]"
    exit 1
fi

TOPIC=$1
MAX_MSGS=${2:-1}

echo "--------------------------------------------------------"
echo "🔍 EXPLORADOR DE KAFKA: $TOPIC"
echo "--------------------------------------------------------"

# 1. Comando base
# Quitamos el flag -t de docker exec y añadimos configuraciones de consumidor
if [[ "$TOPIC" == *"gold"* ]] || [[ "$TOPIC" == *"calendar"* ]]; then
    echo "📡 Formato: AVRO (Capa Gold)"
    CMD="kafka-avro-console-consumer --bootstrap-server kafka:29092 --topic $TOPIC --from-beginning --property schema.registry.url=http://schema-registry:8081 --max-messages $MAX_MSGS"
else
    echo "📄 Formato: JSON/Texto"
    CMD="kafka-console-consumer --bootstrap-server kafka:29092 --topic $TOPIC --from-beginning --max-messages $MAX_MSGS"
fi

# 2. Ejecución con limpieza agresiva de espacios:
# - docker exec -i (sin -t para evitar el ruido de la TTY)
# - xargs -0 (opcional) o simplemente un sed para quitar espacios al inicio de línea
# - jq para el formateo final
docker exec -i "$SR_CONTAINER" $CMD 2>/dev/null | \
    grep --line-buffered '^{' | \
    sed -e 's/^[[:space:]]*//' | \
    tr -d '\r' | \
    jq --color-output .

echo -e "\n--------------------------------------------------------"
echo "✅ Visualización finalizada."
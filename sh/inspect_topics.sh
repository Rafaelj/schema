#!/bin/bash

# Este script lê um arquivo de tópicos, um por linha, e
# chama o script Python 'kafka_schema_inspector.py' para cada um.

# Nome do arquivo de tópicos. Altere para o seu arquivo, se necessário.
TOPICS_FILE="topics.txt"

# Caminho para o seu script Python
PYTHON_SCRIPT="kafka_schema_inspector.py"

# Caminho para o interpretador Python (necessário para o seu ambiente).
# Use o mesmo caminho que funcionou para você.
PYTHON_PATH="/opt/homebrew/bin/python3"

# Verifica se o arquivo de tópicos existe
if [ ! -f "$TOPICS_FILE" ]; then
    echo "Erro: Arquivo '$TOPICS_FILE' não encontrado."
    exit 1
fi

# Lê o arquivo linha por linha
while IFS= read -r TOPIC_NAME; do
    # Ignora linhas em branco ou comentários
    if [[ -z "$TOPIC_NAME" || "$TOPIC_NAME" =~ ^# ]]; then
        continue
    fi

    echo "=================================================="
    echo "Iniciando inspeção do tópico: $TOPIC_NAME"
    echo "=================================================="

    # Chama o script Python com o tópico atual
    # Você pode adicionar as opções --brokers ou --num-messages aqui, se precisar
    "$PYTHON_PATH" "$PYTHON_SCRIPT" "$TOPIC_NAME"

    echo "" # Adiciona uma linha em branco para melhor visualização
done < "$TOPICS_FILE"

echo "Processo de inspeção concluído."
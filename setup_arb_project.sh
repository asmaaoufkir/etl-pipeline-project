#!/bin/bash

# Création du dossier racine
mkdir -p etl-pipeline-project

cd etl-pipeline-project || exit

# --- Ansible ---
mkdir -p ansible/roles/{docker,elasticsearch,kibana}
touch ansible/inventory.yml ansible/playbook.yml

# --- Data ---
mkdir -p data/sample_data
touch data/generate_data.py

# --- Elasticsearch ---
mkdir -p elasticsearch/config

# --- Kibana ---
mkdir -p kibana/config

# --- Logstash ---
mkdir -p logstash/{config,pipelines}

# --- PySpark ---
mkdir -p pyspark/utils
touch pyspark/advanced_etl.py pyspark/requirements.txt

# --- Fichiers à la racine ---
touch docker-compose.yml gitlab-ci.yml README.md

echo "Arborescence ETL créée avec succès ✅"


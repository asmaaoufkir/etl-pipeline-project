#!/usr/bin/env python3
"""
Script de génération de données de logs d'applications
Génère un volume important de données pour tester la pipeline ETL
"""

import json
import random
import argparse
from datetime import datetime, timedelta
import time
from faker import Faker
import logging
import sys
import os

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

fake = Faker()

# Configuration des applications simulées
APPLICATIONS = [
    "web-app", "mobile-app", "api-gateway", "auth-service", 
    "payment-service", "user-service", "product-catalog", 
    "search-service", "notification-service", "analytics-service"
]

LOG_LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]

ERROR_CODES = [200, 201, 400, 401, 403, 404, 500, 502, 503]

MESSAGES = {
    "DEBUG": [
        "Processing request started",
        "Database query executed",
        "Cache hit for key: {key}",
        "Entering function {function_name}",
        "Request parameters: {params}"
    ],
    "INFO": [
        "User {user_id} logged in successfully",
        "Request completed in {time} ms",
        "New user registered: {user_email}",
        "Payment processed for order {order_id}",
        "Product {product_id} viewed by user {user_id}"
    ],
    "WARN": [
        "Slow database query detected: {query_time} ms",
        "High memory usage: {memory_usage}%",
        "Retrying connection to {service}",
        "Deprecated API called: {endpoint}",
        "Unusual activity detected from IP: {ip_address}"
    ],
    "ERROR": [
        "Database connection failed: {error}",
        "External API timeout: {api_endpoint}",
        "Authentication failed for user {user_id}",
        "Payment declined for order {order_id}",
        "File not found: {filename}"
    ],
    "FATAL": [
        "System out of memory: shutting down",
        "Database cluster unreachable",
        "Critical service {service} failed to start",
        "Disk space exhausted on {device}",
        "Application crash detected: {error}"
    ]
}

def generate_log_entry(timestamp, application):
    """Génère une entrée de log aléatoire"""
    level = random.choice(LOG_LEVELS)
    
    # Génération d'un message approprié au niveau
    message_template = random.choice(MESSAGES[level])
    
    # Remplacement des variables dans le message
    message = message_template.format(
        key=fake.uuid4()[:8],
        function_name=fake.word(),
        params={"param1": fake.word(), "param2": random.randint(1, 100)},
        user_id=fake.uuid4()[:8],
        time=random.randint(10, 2000),
        user_email=fake.email(),
        order_id=fake.uuid4(),
        product_id=fake.uuid4()[:6],
        query_time=random.randint(1000, 5000),
        memory_usage=random.randint(80, 95),
        service=random.choice(["database", "redis", "external-api"]),
        endpoint=fake.uri_path(),
        ip_address=fake.ipv4(),
        error=random.choice(["Timeout", "Connection refused", "Authentication failed"]),
        api_endpoint=fake.uri(),
        filename=fake.file_name(),
        device=fake.hostname()
    )
    
    # Détermination du code d'erreur en fonction du niveau de log
    if level in ["DEBUG", "INFO"]:
        error_code = random.choice([200, 201])
    elif level == "WARN":
        error_code = random.choice([200, 201, 400, 401])
    else:
        error_code = random.choice([400, 401, 403, 404, 500, 502, 503])
    
    return {
        "timestamp": timestamp.isoformat(),
        "level": level,
        "application": application,
        "message": message,
        "user_id": fake.uuid4()[:8] if random.random() > 0.3 else None,
        "session_id": fake.uuid4()[:12] if random.random() > 0.5 else None,
        "response_time": random.expovariate(1/100) * 1000,  # Distribution exponentielle
        "error_code": error_code
    }

def generate_data(size, output_file):
    """Génère un volume de données en fonction de la taille demandée"""
    
    # Définition de la taille en fonction de l'argument
    if size == "small":
        num_entries = 10_000
        start_date = datetime.now() - timedelta(days=1)
    elif size == "medium":
        num_entries = 100_000
        start_date = datetime.now() - timedelta(days=7)
    elif size == "large":
        num_entries = 5_000_000
        start_date = datetime.now() - timedelta(days=30)
    else:
        num_entries = 10_000_000
        start_date = datetime.now() - timedelta(days=90)
    
    logger.info(f"Génération de {num_entries:,} entrées de log")
    logger.info(f"Période couverte: du {start_date.date()} à aujourd'hui")
    
    # Calcul de l'intervalle entre les logs
    time_span = (datetime.now() - start_date).total_seconds()
    avg_interval = time_span / num_entries
    
    # Création du répertoire de sortie si nécessaire
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Génération des données
    current_time = start_date
    progress_interval = max(1, num_entries // 100)  # Afficher une progression tous les 1%
    
    with open(output_file, 'w') as f:
        for i in range(num_entries):
            # Génération d'une entrée de log
            application = random.choice(APPLICATIONS)
            log_entry = generate_log_entry(current_time, application)
            
            # Écriture dans le fichier
            f.write(json.dumps(log_entry) + '\n')
            
            # Mise à jour du temps pour la prochaine entrée
            time_increment = random.expovariate(1/avg_interval)
            current_time += timedelta(seconds=time_increment)
            
            # Affichage de la progression
            if i % progress_interval == 0:
                progress = (i / num_entries) * 100
                logger.info(f"Progression: {progress:.1f}% ({i:,}/{num_entries:,} entrées)")
    
    logger.info(f"Génération terminée. Fichier créé: {output_file}")
    logger.info(f"Taille du fichier: {os.path.getsize(output_file) / (1024*1024):.2f} MB")

def main():
    parser = argparse.ArgumentParser(description="Générateur de données de logs d'applications")
    parser.add_argument("--size", choices=["small", "medium", "large", "xlarge"], 
                       default="medium", help="Taille du dataset à générer")
    parser.add_argument("--output", required=True, help="Chemin du fichier de sortie")
    
    args = parser.parse_args()
    
    start_time = time.time()
    generate_data(args.size, args.output)
    end_time = time.time()
    
    logger.info(f"Temps d'exécution: {end_time - start_time:.2f} secondes")

if __name__ == "__main__":
    main()

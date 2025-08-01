import json
import random
import time
from datetime import datetime

from confluent_kafka import Producer

print("Kafka Producer (Realtime Simulator) script starting...")

# --- Configuration Kafka ---
# REMPLACEZ CES VALEURS PAR LES VÔTRES SI VOTRE BROKER N'EST PAS LOCALHOST:9092
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Ex: 'your-kafka-broker-address:9092'
KAFKA_TOPIC_TELEMETRY = 'telemetry_events'  # Topic pour les données de télémétrie
KAFKA_TOPIC_EVENTS = 'maintenance_events'  # Topic pour les événements (erreurs, maintenance)

# Configuration du producteur
producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'realtime-simulator-producer',
}

# Initialiser le producteur Kafka
try:
    producer = Producer(producer_conf)
    print(f"Kafka Producer initialized. Connecting to {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"Error initializing Kafka Producer: {e}")
    print("Please check your Kafka broker address and security configurations.")
    exit()


# --- Fonction de callback pour la livraison des messages ---
def delivery_report(err, msg):
    """ Callback appelé une fois que le message a été livré ou a échoué. """
    if err is not None:
        pass
    else:
        pass


# --- Simulation de données Structurées (Télémétrie, Erreurs, Maintenance) ---
num_machines = 5  # Simuler 5 machines

print(f"Simulating realtime data for {num_machines} machines. Press Ctrl+C to stop.")

try:
    while True:  # Boucle infinie pour simuler un flux continu
        current_time = datetime.now()  # Utilise le timestamp actuel pour chaque cycle

        for machine_id in range(1, num_machines + 1):
            # Simuler les données de télémétrie structurées
            telemetry_data = {
                "type": "telemetry",
                "machineID": machine_id,
                "datetime": current_time.isoformat(),  # Timestamp actuel
                "volt": random.uniform(150, 180),
                "rotate": random.uniform(400, 500),
                "pressure": random.uniform(90, 110),
                "vibration": random.uniform(35, 45)
            }
            # Envoyer le message de télémétrie à Kafka
            producer.produce(
                KAFKA_TOPIC_TELEMETRY,
                key=str(machine_id),
                value=json.dumps(telemetry_data).encode('utf-8'),
                callback=delivery_report
            )

            # Simuler un événement d'erreur occasionnel
            # PROBABILITÉ MODIFIÉE ICI : 20% de chance d'une erreur
            if random.random() < 0.20:
                error_type = random.choice(['error1', 'error2', 'error3', 'error4', 'error5'])
                error_data = {
                    "type": "error",
                    "machineID": machine_id,
                    "datetime": current_time.isoformat(),  # Timestamp actuel
                    "errorID": error_type
                }
                # Envoyer le message d'erreur à Kafka
                producer.produce(
                    KAFKA_TOPIC_EVENTS,
                    key=str(machine_id),
                    value=json.dumps(error_data).encode('utf-8'),
                    callback=delivery_report
                )

            # Simuler un événement de maintenance occasionnel
            # PROBABILITÉ MODIFIÉE ICI : 5% de chance d'une maintenance (ajusté pour être plus fréquent)
            if random.random() < 0.05:
                comp_type = random.choice(['comp1', 'comp2', 'comp3', 'comp4'])
                maint_data = {
                    "type": "maintenance",
                    "machineID": machine_id,
                    "datetime": current_time.isoformat(),  # Timestamp actuel
                    "comp": comp_type
                }
                # Envoyer le message de maintenance à Kafka
                producer.produce(
                    KAFKA_TOPIC_EVENTS,
                    key=str(machine_id),
                    value=json.dumps(maint_data).encode('utf-8'),
                    callback=delivery_report
                )

        producer.poll(0)  # Déclencher les callbacks de livraison
        time.sleep(1)  # Attendre 1 seconde avant le prochain cycle (pour simuler un flux continu)

except KeyboardInterrupt:
    print("\nSimulation stopped by user.")
finally:
    producer.flush()  # S'assurer que tous les messages en attente sont envoyés
    print("Kafka Producer flushed and closed.")
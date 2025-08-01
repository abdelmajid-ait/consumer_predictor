import json
import os
import sys
from datetime import datetime, timedelta

import joblib
import pandas as pd
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError

print("Kafka Consumer (Model Integrator with MongoDB) script starting...")

# --- Configuration Kafka ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_TELEMETRY = 'telemetry_events'
KAFKA_TOPIC_EVENTS = 'maintenance_events'
KAFKA_GROUP_ID = 'predictive_maintenance_mongodb_consumer_group'

# --- Chemin du Modèle et des Données Statiques ---
BASE_LOCAL_PATH = 'C:/Users/hp/Desktop/Stage/'  # MODIFIER ICI POUR VOTRE CHEMIN LOCAL
MODEL_FILENAME = os.path.join(BASE_LOCAL_PATH, 'predictive_maintenance_model.joblib')

PREDICTION_HORIZON_HOURS = 24

# --- Configuration MongoDB ---
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
MONGO_DB = 'predictive_maintenance_db'

COLLECTION_PREDICTIONS = 'predictions'
COLLECTION_TELEMETRY = 'telemetry_events'
COLLECTION_ALERTS = 'alerts'
COLLECTION_SETTINGS = 'settings'  # AJOUTÉ: Collection pour les paramètres

# ALERT_THRESHOLD est maintenant récupéré dynamiquement, donc plus besoin d'une valeur fixe ici
# ALERT_THRESHOLD = 0.70

# --- Initialisation MongoDB Client ---
mongo_client = None
mongo_db = None  # Ajouté pour être accessible globalement
db_predictions_collection = None
db_telemetry_collection = None
db_alerts_collection = None
db_settings_collection = None  # AJOUTÉ: Collection des paramètres

try:
    mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
    mongo_client.admin.command('ping')  # Vérifie la connexion

    mongo_db = mongo_client[MONGO_DB]  # Assigne mongo_db ici

    db_predictions_collection = mongo_db[COLLECTION_PREDICTIONS]
    db_telemetry_collection = mongo_db[COLLECTION_TELEMETRY]
    db_alerts_collection = mongo_db[COLLECTION_ALERTS]
    db_settings_collection = mongo_db[COLLECTION_SETTINGS]  # Initialise la collection settings

    print(f"DEBUG MongoDB: Client initialized. Connecting to {MONGO_HOST}:{MONGO_PORT}, DB: {MONGO_DB}")
    print(
        f"DEBUG MongoDB: Collections initialized: {COLLECTION_PREDICTIONS}, {COLLECTION_TELEMETRY}, {COLLECTION_ALERTS}, {COLLECTION_SETTINGS}")

    existing_collections = mongo_db.list_collection_names()
    print(f"DEBUG MongoDB: Existing collections in DB '{MONGO_DB}' after init: {existing_collections}")

except ConnectionFailure as e:
    print(f"ERROR MongoDB: Could not connect to MongoDB: {e}")
    print("Please ensure MongoDB is running and accessible at the specified host and port.")
    sys.exit(1)
except PyMongoError as e:
    print(f"ERROR MongoDB: An unexpected MongoDB error occurred during MongoDB client initialization: {e}")
    sys.exit(1)


# --- Fonction pour récupérer le seuil d'alerte depuis MongoDB ---
def get_current_alert_threshold_from_db():
    # MODIFIÉ : Vérifier explicitement si db_settings_collection n'est PAS None
    if db_settings_collection is not None:
        try:
            settings_doc = db_settings_collection.find_one({"_id": "global_settings"})
            # Utilise 0.85 comme valeur par défaut si le document ou le champ n'existe pas
            alert_threshold = settings_doc.get("alert_threshold", 0.85) if settings_doc else 0.85
            # print(f"DEBUG Settings: Retrieved alert_threshold from DB: {alert_threshold}") # Décommenter pour un debug intensif
            return alert_threshold
        except PyMongoError as e:
            print(f"ERROR Settings: Could not retrieve alert_threshold from MongoDB: {e}. Using default 0.85.")
            return 0.85
    else:
        # Cette partie du else ne devrait se produire que si db_settings_collection
        # n'a pas pu être initialisée du tout au début du script.
        print("WARNING Settings: MongoDB settings collection not initialized. Using default alert_threshold 0.85.")
        return 0.85


# --- Chargement du Modèle ---
loaded_model = None
MODEL_FEATURE_NAMES = None
try:
    loaded_model = joblib.load(MODEL_FILENAME)
    print(f"DEBUG Model: Model loaded successfully from {MODEL_FILENAME}.")
    try:
        if hasattr(loaded_model, 'feature_names_in_') and loaded_model.feature_names_in_ is not None:
            MODEL_FEATURE_NAMES = loaded_model.feature_names_in_.tolist()
            print("DEBUG Model: Model feature names retrieved from loaded model.")
        else:
            raise AttributeError("feature_names_in_ not found or is None.")
    except (FileNotFoundError, AttributeError):
        print(
            "WARNING Model: 'feature_names_in_' not found in model or separate feature names file. Attempting to reconstruct feature names from a dummy DataFrame. ENSURE THIS MATCHES YOUR TRAINING FEATURES!")
        dummy_feature_columns = [
            'volt', 'rotate', 'pressure', 'vibration', 'age',
            'volt_mean_3h', 'volt_std_3h', 'volt_mean_24h', 'volt_std_24h', 'volt_mean_48h', 'volt_std_48h',
            'rotate_mean_3h', 'rotate_std_3h', 'rotate_mean_24h', 'rotate_std_24h', 'rotate_mean_48h', 'rotate_std_48h',
            'pressure_mean_3h', 'pressure_std_3h', 'pressure_mean_24h', 'pressure_std_24h', 'pressure_mean_48h',
            'pressure_std_48h',
            'vibration_mean_3h', 'vibration_std_3h', 'vibration_mean_24h', 'vibration_std_24h', 'vibration_mean_48h',
            'vibration_std_48h',
            'error_count_sum_3h', 'error_count_sum_24h', 'error_count_sum_48h',
            'time_since_last_maint_comp1', 'time_since_last_maint_comp2',
            'time_since_last_maint_comp3', 'time_since_last_maint_comp4',
            'model_model1', 'model_model2', 'model_model3', 'model_model4'
        ]
        MODEL_FEATURE_NAMES = dummy_feature_columns
        print("DEBUG Model: Reconstructed feature names for model.")

except FileNotFoundError:
    print(f"ERROR Model: Model file not found at {MODEL_FILENAME}. Please ensure it's trained and saved.")
    sys.exit(1)
except Exception as e:
    print(f"ERROR Model: An unexpected error occurred during model loading: {e}")
    sys.exit(1)

# --- Configuration du Consommateur Kafka ---
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'max.poll.interval.ms': 120000,
}

try:
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC_TELEMETRY, KAFKA_TOPIC_EVENTS])
    print(f"DEBUG Kafka: Consumer initialized. Subscribed to {KAFKA_TOPIC_TELEMETRY} and {KAFKA_TOPIC_EVENTS}.")
except Exception as e:
    print(f"ERROR Kafka: Error initializing Kafka Consumer: {e}")
    print("Please check your Kafka broker address, topic names, and security configurations.")
    sys.exit(1)

# --- Gestion de l'Historique des Données pour l'Ingénierie des Fonctionnalités ---
machine_data_state = {}
MAX_HISTORY_HOURS = 48
MIN_TELEMETRY_RECORDS_FOR_PREDICTION = 5

# --- Chargement des métadonnées des machines (statiques) ---
machines_df = None
try:
    machines_df = pd.read_csv(os.path.join(BASE_LOCAL_PATH, 'maintenance_data', 'PdM_machines.csv'))
    machines_df['machineID'] = machines_df['machineID'].astype(int)
    if 'model' not in machines_df.columns:
        print("ERROR Data: 'model' column not found in machines.csv. This is critical for feature engineering.")
        sys.exit(1)
    print("DEBUG Data: Machine metadata loaded.")
except FileNotFoundError:
    print(
        f"ERROR Data: Machine metadata file not found at {os.path.join(BASE_LOCAL_PATH, 'maintenance_data', 'PdM_machines.csv')}. Cannot proceed.")
    sys.exit(1)
except Exception as e:
    print(f"ERROR Data: Error loading machine metadata: {e}")
    sys.exit(1)


# --- Fonction d'Ingénierie des Fonctionnalités ---
def engineer_features_for_inference(machine_id, current_datetime, telemetry_history_df, error_history_df,
                                    maint_last_times_dict, machines_df):
    print(f"  DEBUG Features: Entering engineer_features_for_inference for machine {machine_id} at {current_datetime}")

    if telemetry_history_df.empty:
        print(f"  DEBUG Features: Telemetry history is empty for machine {machine_id}. Cannot compute features.")
        return pd.DataFrame(columns=MODEL_FEATURE_NAMES)

    telemetry_history_df['datetime'] = pd.to_datetime(telemetry_history_df['datetime'])
    telemetry_history_df = telemetry_history_df.sort_values(by='datetime').drop_duplicates(subset='datetime',
                                                                                           keep='last')
    if telemetry_history_df.empty:  # After cleaning, it might become empty
        print(f"  DEBUG Features: Telemetry history became empty after sorting/deduplicating for machine {machine_id}.")
        return pd.DataFrame(columns=MODEL_FEATURE_NAMES)

    telemetry_history_df_indexed = telemetry_history_df.set_index('datetime').copy()
    current_telemetry_point_data = telemetry_history_df.iloc[[-1]].copy()

    machine_meta = machines_df[machines_df['machineID'] == machine_id]

    if machine_meta.empty:
        print(
            f"  WARNING Features: Machine {machine_id} metadata not found in PdM_machines.csv. Returning empty DataFrame.")
        return pd.DataFrame(columns=MODEL_FEATURE_NAMES)

    processed_point = pd.merge(current_telemetry_point_data, machine_meta, on='machineID', how='left')

    # Correction pour le calcul de 'age' : Utiliser la date de fabrication si elle existe, sinon un défaut raisonnable
    # Vérifier si 'year' et 'month' sont présents ou si un champ de date de fabrication existe
    # Supposons qu'PdM_machines.csv contienne 'year' et 'month' comme la date de fabrication.
    # Si ce n'est pas le cas, vous devrez ajuster comment 'age' est dérivé.
    if 'year' in processed_point.columns and 'month' in processed_point.columns:
        # Assurez-vous que year et month sont numériques
        try:
            mfg_year = int(processed_point['year'].iloc[0])
            mfg_month = int(processed_point['month'].iloc[0])
            processed_point['age'] = (current_datetime.year - mfg_year) * 12 + (current_datetime.month - mfg_month)
        except ValueError:
            print(
                f"  WARNING Features: 'year' or 'month' in machine metadata are not numeric for machine {machine_id}. Defaulting age calculation.")
            processed_point['age'] = (current_datetime.year - 2018) * 12 + (
                        current_datetime.month - 1)  # Exemple: 2018-01 comme défaut
    else:
        print(
            f"  WARNING Features: 'year' or 'month' columns missing in machine metadata for machine {machine_id}. Defaulting age calculation.")
        processed_point['age'] = (current_datetime.year - 2018) * 12 + (
                    current_datetime.month - 1)  # Exemple: 2018-01 comme défaut

    processed_point['model'] = processed_point['model'].astype(str)

    telemetry_numerical_features = ['volt', 'rotate', 'pressure', 'vibration']
    window_sizes = [3, 24, 48]

    for feature in telemetry_numerical_features:
        for window in window_sizes:
            rolling_mean = telemetry_history_df_indexed[feature].rolling(
                window=f'{window}h', closed='right', min_periods=1
            ).mean()
            rolling_std = telemetry_history_df_indexed[feature].rolling(
                window=f'{window}h', closed='right', min_periods=1
            ).std()

            processed_point[f'{feature}_mean_{window}h'] = rolling_mean.iloc[-1] if not rolling_mean.empty else 0
            processed_point[f'{feature}_std_{window}h'] = rolling_std.iloc[-1] if not rolling_std.empty else 0

    for feature in telemetry_numerical_features:
        for window in window_sizes:
            std_col = f'{feature}_std_{window}h'
            if std_col in processed_point.columns:
                # Si la STD est NaN (souvent quand il y a un seul point dans la fenêtre), la mettre à 0
                if pd.isna(processed_point[std_col].iloc[0]):
                    processed_point[std_col] = 0
                # Si toutes les valeurs dans la fenêtre sont identiques (STD = 0)
                recent_history_for_std_check = telemetry_history_df_indexed[
                    (telemetry_history_df_indexed.index > current_datetime - timedelta(hours=window)) &
                    (telemetry_history_df_indexed.index <= current_datetime)
                    ]
                if len(recent_history_for_std_check) > 0 and len(recent_history_for_std_check[feature].unique()) <= 1:
                    processed_point[std_col] = 0

    if not error_history_df.empty:
        error_history_df['datetime'] = pd.to_datetime(error_history_df['datetime'])
        error_history_df = error_history_df.sort_values(by='datetime').drop_duplicates(subset='datetime', keep='last')

        error_history_df_indexed = error_history_df.set_index('datetime').copy()

        for window in window_sizes:
            errors_in_window = error_history_df_indexed[
                (error_history_df_indexed.index > current_datetime - timedelta(hours=window)) &
                (error_history_df_indexed.index <= current_datetime)
                ].shape[0]
            processed_point[f'error_count_sum_{window}h'] = errors_in_window
    else:
        for window in window_sizes:
            processed_point[f'error_count_sum_{window}h'] = 0

    for comp in ['comp1', 'comp2', 'comp3', 'comp4']:
        last_maint_time = maint_last_times_dict.get(comp, None)
        if last_maint_time and current_datetime >= last_maint_time:
            time_diff_hours = (current_datetime - last_maint_time).total_seconds() / 3600
            processed_point[f'time_since_last_maint_{comp}'] = time_diff_hours
        else:
            # Si pas de maintenance ou maintenance future (ce qui ne devrait pas arriver ici)
            processed_point[
                f'time_since_last_maint_{comp}'] = 999999  # Grande valeur pour indiquer "pas de maintenance récente"

    model_dummies = pd.get_dummies(processed_point['model'], prefix='model')
    # S'assurer que toutes les colonnes de dummy de modèle possibles sont présentes
    # pour maintenir la cohérence des caractéristiques
    for col in [f'model_model{i}' for i in range(1, 5)]:  # Modèles 1 à 4
        if col not in model_dummies.columns:
            model_dummies[col] = 0

    # Supprimer les colonnes dupliquées si pd.get_dummies les crée (par ex. si 'model' est déjà un dummy)
    # Et sélectionner uniquement les colonnes de dummy que le modèle attend
    expected_model_dummies = [f'model_model{i}' for i in range(1, 5)]
    model_dummies = model_dummies[expected_model_dummies]

    processed_point = pd.concat(
        [processed_point.drop(columns=['model', 'year', 'month'], errors='ignore'), model_dummies], axis=1)

    cols_to_drop = ['datetime', 'machineID']
    processed_point = processed_point.drop(columns=cols_to_drop, errors='ignore')

    if MODEL_FEATURE_NAMES:
        # S'assurer que toutes les colonnes attendues par le modèle sont présentes
        missing_cols = set(MODEL_FEATURE_NAMES) - set(processed_point.columns)
        for c in missing_cols:
            processed_point[c] = 0  # Remplir les colonnes manquantes avec 0

        # Supprimer les colonnes qui ne sont pas attendues par le modèle
        extra_cols = set(processed_point.columns) - set(MODEL_FEATURE_NAMES)
        processed_point = processed_point.drop(columns=list(extra_cols), errors='ignore')

        # Réordonner les colonnes pour qu'elles correspondent à l'ordre d'entraînement du modèle
        processed_point = processed_point[MODEL_FEATURE_NAMES]

    processed_point.fillna(0, inplace=True)  # Gérer toutes les NaN restantes

    print(
        f"  DEBUG Features: Exiting engineer_features_for_inference for machine {machine_id}. Engineered features shape: {processed_point.shape}")
    print(f"  DEBUG Features: First few engineered features values:\n{processed_point.head(1)}")

    return processed_point


# --- Fonction de parsing du datetime ---
def parse_datetime(dt_str):
    if isinstance(dt_str, datetime):
        return dt_str
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except ValueError:
        try:
            # Tente de convertir une chaîne qui pourrait être un timestamp UNIX
            return datetime.fromtimestamp(float(dt_str) / 1000)
        except ValueError:
            print(f"WARNING Datetime: Could not parse datetime string '{dt_str}'. Returning current time.")
            return datetime.now()


# --- Boucle Principale du Consommateur ---
running = True


def stop_consumer():
    global running
    running = False
    print("\nStopping Kafka consumer...")


print("Starting Kafka Consumer loop. Press Ctrl+C to stop.")

# Variable pour stocker le seuil d'alerte actuel
current_alert_threshold = get_current_alert_threshold_from_db()
last_threshold_check_time = datetime.now()
THRESHOLD_REFRESH_INTERVAL_SECONDS = 300  # Vérifie le seuil toutes les 5 minutes

try:
    while running:
        # Vérifier et rafraîchir le seuil d'alerte périodiquement
        if (datetime.now() - last_threshold_check_time).total_seconds() > THRESHOLD_REFRESH_INTERVAL_SECONDS:
            new_threshold = get_current_alert_threshold_from_db()
            if new_threshold != current_alert_threshold:
                print(f"INFO Settings: Alert threshold updated from {current_alert_threshold} to {new_threshold}.")
                current_alert_threshold = new_threshold
            last_threshold_check_time = datetime.now()

        msg = consumer.poll(timeout=1.0)  # Augmente le timeout si tu penses que Kafka est lent

        if msg is None:
            # print("DEBUG Kafka: No message received (poll timeout).") # Décommenter pour voir chaque poll vide
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # print("DEBUG Kafka: End of partition reached (Kafka rebalance or no more messages).")
                pass
            else:
                print(f"ERROR Kafka: Kafka consumer error: {msg.error()}")
                continue
        else:
            print(f"\n--- DEBUG Consumer: Message received from topic: {msg.topic()} ---")
            try:
                record = json.loads(msg.value().decode('utf-8'))
                topic = msg.topic()

                print(f"  DEBUG Consumer: Decoded message: {record}")

                message_type = record.get('type')
                machine_id_raw = record.get('machineID')
                datetime_data = record.get('datetime')

                if not machine_id_raw or not datetime_data or not message_type:
                    print(
                        f"  WARNING Consumer: Skipping malformed message from topic {topic}. Missing machineID, datetime or type. Record: {record}")
                    continue

                try:
                    machine_id = int(machine_id_raw)
                except ValueError:
                    print(
                        f"  ERROR Consumer: Invalid machineID received (not an integer): {machine_id_raw}. Skipping message.")
                    continue

                current_datetime = parse_datetime(datetime_data)
                print(
                    f"  DEBUG Consumer: Processed - Machine ID: {machine_id}, Type: {message_type}, Datetime: {current_datetime.strftime('%Y-%m-%d %H:%M:%S')}")

                if machine_id not in machine_data_state:
                    machine_data_state[machine_id] = {
                        'telemetry_history_df': pd.DataFrame(
                            columns=['datetime', 'machineID', 'volt', 'rotate', 'pressure', 'vibration']),
                        'error_history_df': pd.DataFrame(columns=['datetime', 'errorID', 'machineID']),
                        'maint_last_times': {}
                    }
                    print(f"  DEBUG Consumer: Initialized new history state for machine {machine_id}.")

                if message_type == "telemetry":
                    print(f"  DEBUG Consumer: Handling telemetry message for machine {machine_id}.")
                    required_telemetry_cols = ['machineID', 'datetime', 'volt', 'rotate', 'pressure', 'vibration']
                    if not all(col in record for col in required_telemetry_cols):
                        print(
                            f"  WARNING Telemetry: Incomplete telemetry message for machine {machine_id}. Required fields: {required_telemetry_cols}. Record: {record}")
                        continue

                    telemetry_doc = {
                        "machineID": machine_id,
                        "datetime": current_datetime,
                        "volt": record.get("volt"),
                        "rotate": record.get("rotate"),
                        "pressure": record.get("pressure"),
                        "vibration": record.get("vibration")
                    }
                    try:
                        result = db_telemetry_collection.insert_one(telemetry_doc)
                        print(
                            f"  DEBUG MongoDB Insert: Telemetry for machine {machine_id} at {current_datetime.strftime('%H:%M:%S')} STORED IN {COLLECTION_TELEMETRY}. Inserted ID: {result.inserted_id}")
                    except PyMongoError as e:
                        print(
                            f"  ERROR MongoDB Insert: Could not insert telemetry for machine {machine_id} into MongoDB collection '{COLLECTION_TELEMETRY}': {e}")
                        import traceback;

                        traceback.print_exc()

                    new_telemetry_row = pd.DataFrame([{
                        'datetime': current_datetime,
                        'machineID': machine_id,
                        'volt': record.get('volt'),
                        'rotate': record.get('rotate'),
                        'pressure': record.get('pressure'),
                        'vibration': record.get('vibration')
                    }])

                    machine_data_state[machine_id]['telemetry_history_df'] = pd.concat([
                        machine_data_state[machine_id]['telemetry_history_df'],
                        new_telemetry_row
                    ], ignore_index=True)

                    cutoff_time = current_datetime - timedelta(hours=MAX_HISTORY_HOURS)
                    machine_data_state[machine_id]['telemetry_history_df'] = \
                        machine_data_state[machine_id]['telemetry_history_df'][
                            machine_data_state[machine_id]['telemetry_history_df']['datetime'] >= cutoff_time
                            ].copy()
                    machine_data_state[machine_id]['telemetry_history_df'] = machine_data_state[machine_id][
                        'telemetry_history_df'].sort_values(by='datetime').reset_index(drop=True)

                    print(
                        f"  DEBUG Telemetry: Machine {machine_id} telemetry history size: {len(machine_data_state[machine_id]['telemetry_history_df'])} records.")

                    if len(machine_data_state[machine_id][
                               'telemetry_history_df']) >= MIN_TELEMETRY_RECORDS_FOR_PREDICTION:
                        print(
                            f"  DEBUG Prediction: Enough telemetry history ({len(machine_data_state[machine_id]['telemetry_history_df'])} records) for machine {machine_id}. Attempting feature engineering...")
                        X_inference_features = engineer_features_for_inference(
                            machine_id,
                            current_datetime,
                            machine_data_state[machine_id]['telemetry_history_df'],
                            machine_data_state[machine_id]['error_history_df'],
                            machine_data_state[machine_id]['maint_last_times'],
                            machines_df
                        )

                        if not X_inference_features.empty and loaded_model and MODEL_FEATURE_NAMES:
                            print(
                                f"  DEBUG Prediction: Features engineered successfully for machine {machine_id}. Proceeding with prediction.")
                            try:
                                if len(X_inference_features) == 0:
                                    print(
                                        f"  WARNING Prediction: Skipping prediction: engineered features for machine {machine_id} are empty after engineering.")
                                    continue

                                X_inference_features = X_inference_features[MODEL_FEATURE_NAMES]

                                if hasattr(loaded_model, 'predict_proba'):
                                    probability_of_failure = loaded_model.predict_proba(X_inference_features)[:, 1][0]
                                else:
                                    print(
                                        "  WARNING Prediction: Model does not have predict_proba method. Using predict as fallback.")
                                    raw_prediction = loaded_model.predict(X_inference_features)[0]
                                    probability_of_failure = float(raw_prediction) if isinstance(raw_prediction,
                                                                                                 (int, float)) else (
                                        1.0 if raw_prediction else 0.0)

                                model_threshold = getattr(loaded_model, 'threshold', 0.5)
                                predicted_failure_bool = probability_of_failure >= model_threshold

                                print(
                                    f"  DEBUG Prediction: Prediction result for Machine {machine_id} at {current_datetime.strftime('%Y-%m-%d %H:%M:%S')}:")
                                print(
                                    f"    Prob. of Failure: {probability_of_failure:.4f} (Predicted: {predicted_failure_bool}, Model Threshold: {model_threshold})")

                                prediction_document = {
                                    "machineID": machine_id,
                                    "datetime": current_datetime,
                                    "probability_of_failure": float(probability_of_failure),
                                    "predicted_failure": bool(predicted_failure_bool),
                                    "prediction_timestamp": datetime.now()
                                }

                                try:
                                    result = db_predictions_collection.insert_one(prediction_document)
                                    print(
                                        f"  DEBUG MongoDB Insert: Prediction for machine {machine_id} WRITTEN TO {COLLECTION_PREDICTIONS}. Inserted ID: {result.inserted_id}")
                                except PyMongoError as e:
                                    print(
                                        f"  ERROR MongoDB Insert: Could not insert prediction for machine {machine_id} into MongoDB collection '{COLLECTION_PREDICTIONS}': {e}")
                                    import traceback;

                                    traceback.print_exc()
                                except Exception as e:
                                    print(
                                        f"  FATAL ERROR MongoDB Insert: An unexpected non-PyMongo error occurred during prediction insertion for machine {machine_id}: {e}")
                                    import traceback;

                                    traceback.print_exc()

                                # MODIFIÉ: Utilise current_alert_threshold qui est rafraîchi périodiquement
                                if probability_of_failure >= current_alert_threshold:
                                    # Vérifie si une alerte active existe déjà pour cette machine et type d'alerte
                                    # Cela évite de créer des doublons d'alertes identiques non résolues
                                    existing_alert = db_alerts_collection.find_one({
                                        "machineID": machine_id,
                                        "status": "active",
                                        "alertType": "Probabilité de défaillance élevée"
                                        # Assurez-vous que c'est le même type que vous utilisez
                                    })

                                    if not existing_alert:
                                        alert_doc = {
                                            "machineID": machine_id,
                                            "datetime": current_datetime,
                                            # Date/heure à laquelle la condition d'alerte a été remplie
                                            "probability_of_failure": float(probability_of_failure),
                                            "status": "active",
                                            "alertType": "Probabilité de défaillance élevée",
                                            # Type d'alerte spécifique
                                            "message": f"Probabilité de défaillance élevée ({probability_of_failure:.2f} >= seuil {current_alert_threshold:.2f})",
                                            "alert_timestamp": datetime.now()  # Date/heure de création de l'alerte
                                        }
                                        try:
                                            result = db_alerts_collection.insert_one(alert_doc)
                                            print(
                                                f"  DEBUG MongoDB Insert: !!! ALERTE GENERATED for Machine {machine_id}: High probability ({probability_of_failure:.2f}) !!! Inserted ID: {result.inserted_id}")
                                        except PyMongoError as e:
                                            print(
                                                f"  ERROR MongoDB Insert: Could not insert alert for machine {machine_id} into MongoDB collection '{COLLECTION_ALERTS}': {e}")
                                            import traceback;

                                            traceback.print_exc()
                                    else:
                                        print(
                                            f"  DEBUG Alert: Active alert already exists for machine {machine_id}. Not creating duplicate.")


                            except Exception as pred_e:
                                print(
                                    f"  ERROR Prediction: An exception occurred during prediction or DB write for machine {machine_id}: {pred_e}")
                                import traceback;

                                traceback.print_exc()
                        else:
                            print(
                                f"  DEBUG Prediction: Skipping prediction for machine {machine_id} - Engineered features empty, or model/feature names not ready.")
                    else:
                        print(
                            f"  DEBUG Prediction: Not enough telemetry history for machine {machine_id} to engineer features yet. Needs {MIN_TELEMETRY_RECORDS_FOR_PREDICTION} records. Current: {len(machine_data_state[machine_id]['telemetry_history_df'])}")

                elif message_type == "error":
                    print(f"  DEBUG Consumer: Handling error message for machine {machine_id}.")
                    required_error_cols = ['machineID', 'datetime', 'errorID']
                    if not all(col in record for col in required_error_cols):
                        print(
                            f"  WARNING Error: Incomplete error message for machine {machine_id}. Required fields: {required_error_cols}. Record: {record}")
                        continue

                    new_error_row = pd.DataFrame([record])[required_error_cols]
                    new_error_row['datetime'] = pd.to_datetime(new_error_row['datetime'])

                    machine_data_state[machine_id]['error_history_df'] = pd.concat([
                        machine_data_state[machine_id]['error_history_df'],
                        new_error_row
                    ], ignore_index=True)

                    cutoff_time = current_datetime - timedelta(hours=MAX_HISTORY_HOURS)
                    machine_data_state[machine_id]['error_history_df'] = \
                        machine_data_state[machine_id]['error_history_df'][
                            machine_data_state[machine_id]['error_history_df']['datetime'] >= cutoff_time
                            ].copy()
                    machine_data_state[machine_id]['error_history_df'] = machine_data_state[machine_id][
                        'error_history_df'].sort_values(by='datetime').reset_index(drop=True)

                    print(
                        f"  DEBUG Error: Received error for machine {machine_id}: {record['errorID']} at {current_datetime.strftime('%H:%M:%S')}. History size: {len(machine_data_state[machine_id]['error_history_df'])} records.")

                elif message_type == "maintenance":
                    print(f"  DEBUG Consumer: Handling maintenance message for machine {machine_id}.")
                    required_maint_cols = ['machineID', 'datetime', 'comp']
                    if not all(col in record for col in required_maint_cols):
                        print(
                            f"  WARNING Maintenance: Incomplete maintenance message for machine {machine_id}. Required fields: {required_maint_cols}. Record: {record}")
                        continue

                    comp = record['comp']
                    if comp in ['comp1', 'comp2', 'comp3', 'comp4']:
                        machine_data_state[machine_id]['maint_last_times'][comp] = current_datetime
                        print(
                            f"  DEBUG Maintenance: Received maintenance for machine {machine_id} on {comp} at {current_datetime.strftime('%H:%M:%S')}.")
                    else:
                        print(
                            f"  WARNING Maintenance: Unknown component '{comp}' for maintenance record on machine {machine_id}. Record: {record}")
                else:
                    print(
                        f"  WARNING Consumer: Unknown message type received: '{message_type}' for machine {machine_id}. Message: {record}")

            except json.JSONDecodeError:
                print(
                    f"  ERROR Consumer: Could not decode JSON from message on topic {msg.topic()}: {msg.value().decode('utf-8')}")
            except KeyError as ke:
                print(f"  ERROR Consumer: Missing key in message from topic {msg.topic()}: {ke} - Message: {record}")
            except Exception as e:
                print(
                    f"  FATAL ERROR Consumer: An unexpected error occurred while processing message from topic {msg.topic()}: {e}")
                import traceback

                traceback.print_exc()

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
except KafkaException as e:
    print(f"FATAL Kafka: Kafka error: {e}")
except Exception as e:
    print(f"FATAL Consumer: An unexpected error occurred: {e}")
    import traceback

    traceback.print_exc()
finally:
    consumer.close()
    if mongo_client:
        mongo_client.close()
    print("Kafka Consumer and MongoDB client closed.")
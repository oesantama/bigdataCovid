"""
Script para simular la llegada de nuevos reportes de casos COVID-19
y enviarlos a un topic de Kafka.
"""

import time
import json
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# --- Configuración ---
KAFKA_BROKER = 'localhost:9092'  # Ajusta si tu Kafka no está en localhost
KAFKA_TOPIC = 'covid-colombia-nuevos-casos' # Nombre del topic donde se publicarán los eventos
EVENTS_PER_SECOND = 2  # Número aproximado de eventos a generar por segundo

# Listas de ejemplo para generar datos aleatorios (basadas en Colombia)
DEPARTAMENTOS = ['BOGOTA', 'ANTIOQUIA', 'VALLE DEL CAUCA', 'CUNDINAMARCA', 'SANTANDER', 'ATLANTICO', 'BOLIVAR']
MUNICIPIOS_BY_DEP = {
    'BOGOTA': ['BOGOTA D.C.'],
    'ANTIOQUIA': ['MEDELLIN', 'BELLO', 'ITAGUI', 'ENVIGADO', 'RIONEGRO'],
    'VALLE DEL CAUCA': ['CALI', 'PALMIRA', 'BUENAVENTURA', 'TULUA'],
    'CUNDINAMARCA': ['SOACHA', 'FUSAGASUGA', 'FACATATIVA', 'ZIPAQUIRA'],
    'SANTANDER': ['BUCARAMANGA', 'FLORIDABLANCA', 'GIRON', 'PIEDECUESTA'],
    'ATLANTICO': ['BARRANQUILLA', 'SOLEDAD', 'MALAMBO'],
    'BOLIVAR': ['CARTAGENA', 'MAGANGUE']
}
SEXO = ['F', 'M']
ESTADO = ['Leve', 'Moderado', 'Grave', 'Fallecido'] # Simular el estado inicial reportado
TIPO_CONTAGIO = ['Comunitaria', 'Relacionado', 'Importado', 'En Estudio']
UBICACION = ['Casa', 'Hospital', 'UCI', 'N/A']
RECUPERADO = ['Activo'] # Nuevos casos usualmente empiezan como Activo

# Función para serializar mensajes a JSON y luego a bytes
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Crear productor de Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer,
        # Opciones adicionales para robustez (opcional):
        # acks='all', # Esperar confirmación de todos los in-sync replicas
        # retries=3, # Reintentar envíos fallidos
    )
    print(f"Conectado a Kafka en {KAFKA_BROKER}. Enviando mensajes al topic '{KAFKA_TOPIC}'...")
    print(f"Generando aprox. {EVENTS_PER_SECOND} eventos por segundo. Presiona Ctrl+C para detener.")

    case_counter = 0
    while True:
        case_counter += 1
        report_time = datetime.now()
        dep = random.choice(DEPARTAMENTOS)
        municipio = random.choice(MUNICIPIOS_BY_DEP[dep])

        # Generar un evento de nuevo caso simulado
        event = {
            'fecha_reporte_web': report_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3], # Formato similar al original
            'id_de_caso': str(uuid.uuid4()), # ID único
            'fecha_de_notificaci_n': (report_time - timedelta(days=random.randint(0, 2))).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
            'departamento_nom': dep,
            'ciudad_municipio_nom': municipio,
            'edad': random.randint(0, 95),
            'unidad_medida': 1, # Asumir años para simplificar
            'sexo': random.choice(SEXO),
            'fuente_tipo_contagio': random.choice(TIPO_CONTAGIO),
            'ubicacion': random.choice(UBICACION),
            'estado': random.choice(ESTADO),
            'recuperado': random.choice(RECUPERADO),
            'fecha_inicio_sintomas': (report_time - timedelta(days=random.randint(1, 10))).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] if random.random() > 0.1 else None,
            'fecha_diagnostico': (report_time - timedelta(days=random.randint(0, 3))).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] if random.random() > 0.2 else None,
            # Otros campos podrían ser None o valores por defecto
            'fecha_muerte': None,
            'fecha_recuperado': None,
            'tipo_recuperacion': None,
            'cod_etnia': None,
            'grupo_etnico': None
        }

        # Enviar mensaje
        # print(f"Enviando evento {case_counter}: {event['id_de_caso']} - {event['departamento_nom']}")
        producer.send(KAFKA_TOPIC, value=event)

        # Controlar la tasa de envío
        sleep_time = 1.0 / EVENTS_PER_SECOND
        time.sleep(sleep_time)

except ImportError:
    print("Error: La librería 'kafka-python' no está instalada.")
    print("Por favor, instálala con: pip install kafka-python")
except Exception as e:
    print(f"\nError conectando o enviando a Kafka: {e}")
    print("Asegúrate de que Kafka esté corriendo y accesible en", KAFKA_BROKER)
except KeyboardInterrupt:
    print("\nDeteniendo productor...")
finally:
    if 'producer' in locals() and producer:
        producer.flush() # Asegurar que los últimos mensajes se envíen
        producer.close()
        print("Productor Kafka cerrado.")
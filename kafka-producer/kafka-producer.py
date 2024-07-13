from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import random
import time

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD'),
    'client.id': os.getenv('CLIENT_ID')
}

producer = Producer(**conf)

# Função de callback para entrega de mensagens
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

topic = os.getenv('TOPIC')
for i in range(10):
    temperature = random.uniform(-5,5) ## minha chava
    key = f"{(i % 3) + 1}" ## indentificacao
    producer.produce(topic, key=key, value=f"STRING", callback=delivery_report)
    producer.poll(0)
    time.sleep(1)

# Espera até todas as mensagens serem entregues
producer.flush()
import argparse
import base64
import json
import os
import time
import pika

parser = argparse.ArgumentParser(description='Cliente RabbitMQ')
parser.add_argument('--client-id',type=str,default='1',help='ID do cliente para identificar a pasta')

args = parser.parse_args()

CLIENT_ID = args.client-id
INPUT_DIR = os.environ.get('INPUT_DIR', f'data/clientes/cliente{CLIENT_ID}')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=5672,
    credentials=pika.PlainCredentials(username='guest', password='guest')
)

connection = None
for _ in range(5):
    try:
        connection = pika.BlockingConnection(parameters)
        break
    except pika.exceptions.AMQPConnectionError:
        print('Aguardando RabbitMQ iniciar')
        time.sleep(2)

if not connection:
    print('Erro ao conectar no RabbitMQ.')
    exit(1)

channel = connection.channel()

channel.queue_declare(queue='fila_conversao', durable=True)

if not os.path.exists(INPUT_DIR):
    print(f'Pasta {INPUT_DIR} nao encontrada')
    connection.close()
    exit(1)

extensoes_permitidas = ('.jpg', '.jpeg', '.png')
arquivos = [
    f
    for f in os.listdir(INPUT_DIR)
    if f.lower().endswith(extensoes_permitidas)
]

if not arquivos:
    print(f'Nenhuma imagem encontrada em {INPUT_DIR}')
else:
    print(
        f'Encontradas {len(arquivos)} imagens. Enviando'
    )

for nome_arquivo in arquivos:
    caminho_completo = os.path.join(INPUT_DIR, nome_arquivo)

    with open(caminho_completo, 'rb') as f:
        bytes_imagem = f.read()

    base64_imagem = base64.b64encode(bytes_imagem).decode('utf-8')
    payload = json.dumps({'filename': nome_arquivo, 'content': base64_imagem})

    channel.basic_publish(exchange='', routing_key='fila_conversao',body=payload,properties=pika.BasicProperties(delivery_mode=2))

    print(f'Imagem {nome_arquivo} enviada para a fila_conversao')

connection.close()
print(f'[Cliente {CLIENT_ID}] Envio concluído.')
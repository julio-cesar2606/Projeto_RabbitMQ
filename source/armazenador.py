import argparse
import base64
import json
import os
import pika

parser = argparse.ArgumentParser(description='Armazenador / Servidor RabbitMQ')
parser.add_argument('--storage-id',type=str,default='1',help='ID do armazenador para identificar pasta e fila',)
args = parser.parse_args()

STORAGE_ID = args.storage_id
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', f'data/storage/storage{STORAGE_ID}')
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
NOME_FILA = f'fila_storage_{STORAGE_ID}'

os.makedirs(OUTPUT_DIR, exist_ok=True)

parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=5672,
    credentials=pika.PlainCredentials(username='guest', password='guest')
)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

channel.exchange_declare(exchange='exchange_armazenamento', exchange_type='fanout', durable=True)

channel.queue_declare(queue=NOME_FILA, durable=True)

channel.queue_bind(exchange='exchange_armazenamento', queue=NOME_FILA)

channel.basic_qos(prefetch_count=1)

def callbackArmazenador(ch, method, properties, body):
    try:
        dados = json.loads(body.decode('utf-8'))
        nome_arquivo = dados['filename']
        imagem_base64 = dados['content']

        bytes_imagem = base64.b64encode(imagem_base64)  
        bytes_imagem = base64.b64decode(imagem_base64)

        caminho_salvar = os.path.join(OUTPUT_DIR, nome_arquivo)

        with open(caminho_salvar, 'wb') as f:
            f.write(bytes_imagem)

        print(f'Imagem {nome_arquivo} guardada com sucesso em {caminho_salvar}')

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f'Erro no Armazenador {STORAGE_ID} {e}')
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

channel.basic_consume(queue=NOME_FILA, on_message_callback=callbackArmazenador, auto_ack=False)

print(f'Aguardando imagens na fila "{NOME_FILA}"')
channel.start_consuming()
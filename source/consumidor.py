import os
import pika
import base64
import io
import json
import time
from PIL import Image

# Pega o host do RabbitMQ vindo do docker-compose ou assume 'localhost' 
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')

parameters = pika.ConnectionParameters(
    host=RABBITMQ_HOST,
    port=5672,
    credentials=pika.PlainCredentials(username="guest", password="guest") 
)

connection = None
for i in range(10):
    try:
        connection = pika.BlockingConnection(parameters)
        print(f'Comsumidor conectado ao RabbitMQ')
        break
    except pika.exceptions.AMQPConnectionError:
        print(f'Aguardando RabbitMQ subir({i+1}/10)')
        time.sleep(3)

if not connection:
    print(f'Nao foi possivel conectar ao RabbitMQ')
    exit(1)

channel = connection.channel()

channel.queue_declare(queue="filaConsumidor",durable=True)

channel.exchange_declare(exchange="exchange_armazenamento",exchange_type="fanout",durable=True)

channel.basic_qos(prefetch_count=1)

def minha_callback(ch, method, properties, body):
    try:
        dados=json.loads(body.decode('utf-8'))
        nome_arquivo=dados['filename']
        imagem_base64=dados['content']

        img_bytes = base64.b64decode(imagem_base64)
        imagem = Image.open(io.BytesIO(img_bytes))

        imagem_cinza = imagem.convert('L')

        buffer = io.BytesIO()
        formato = imagem.format if imagem.format else 'JPG'
        imagem_cinza.save(buffer, format=formato)
        img_cinza_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

        mensagem_saida = json.dumps({'filename': nome_arquivo, 'content': img_cinza_base64})

        ch.basic_publish(exchange='exchange_armazenamento', routing_key='', body=mensagem_saida, properties=pika.BasicProperties( delivery_mode=2))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f'[Conversor] Imagem {nome_arquivo} convertida e enviada')

    except Exception as e:
        print(f'Erro no Consumidor {e}')
        # rejeita a mensagem e tenta enfilar de novo
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

channel.basic_consume(
    queue='fila_conversao',
    on_message_callback=minha_callback,
    auto_ack=False,  
)

print('Aguardando imagens para conversao')
channel.start_consuming()
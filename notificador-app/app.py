import os
import json
import logging
import smtplib
from email.message import EmailMessage
from confluent_kafka import Consumer, KafkaError

EMAIL_USER = os.environ['EMAIL_USER']
EMAIL_PASS = os.environ['EMAIL_PASS'].replace(' ', '')
EMAIL_TO = os.environ['EMAIL_TO']


def send_email(text):
    msg = EmailMessage()
    msg['Subject'] = 'Notificação de processamento de imagem'
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_TO
    msg.set_content(text)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(EMAIL_USER, EMAIL_PASS)
        server.send_message(msg)


c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificador-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notificacao'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            text = f"O arquivo {data['file']} foi {data['operation']}."
            logging.warning(f"NOTIFICANDO: {text}")
            try:
                send_email(text)
                logging.warning("E-MAIL ENVIADO")
            except Exception as e:
                logging.error(f"Falha ao enviar e-mail: {e}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached')
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))
except KeyboardInterrupt:
    pass
finally:
    c.close()

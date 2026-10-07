from PIL import Image, ImageDraw, ImageFont
import os
from confluent_kafka import Consumer, KafkaError
import json
import logging

OUT_FOLDER = '/processed/watermark/'
NEW = '_watermark'
IN_FOLDER = "/appdata/static/uploads/"


def get_font(size):
    # Pillow >= 10.1 permite escolher o tamanho da fonte padrão
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def create_watermark(path_file):
    pathname, filename = os.path.split(path_file)
    output_folder = pathname + OUT_FOLDER

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    image = Image.open(path_file).convert("RGB")
    draw = ImageDraw.Draw(image)

    font = get_font(max(14, image.width // 25))
    x, y = 10, 10

    # fundo preto atrás do texto para ficar legível em qualquer imagem
    left, top, right, bottom = draw.textbbox((x, y), filename, font=font)
    draw.rectangle((left - 5, top - 5, right + 5, bottom + 5), fill=(0, 0, 0))
    draw.text((x, y), filename, font=font, fill=(255, 255, 255))

    name, ext = os.path.splitext(filename)
    image.save(output_folder + name + NEW + ext)


c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'watermark-group',   # TEM que ser diferente de rotate-group e grayscale-group
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['image'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            filename = data['new_file']
            logging.warning(f"READING {filename}")
            create_watermark(IN_FOLDER + filename)
            logging.warning(f"ENDING {filename}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()

import os
import re
import psycopg2
import logging
from dotenv import load_dotenv
from telethon import TelegramClient, events


load_dotenv()  # загружаем переменные окружения из файла .env

# получаем значения переменных окружения
API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

logging.basicConfig(filename='logging.log', level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
# создаем подключение к базе данных
# создаем подключение к базе данных
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        options=f'-c search_path=public')
    logger.info("Connected to the PostgreSQL database")
except (Exception, psycopg2.DatabaseError) as error:
    logger.error(error)
    raise error

# создаем курсор для выполнения операций с базой данных
cur = conn.cursor()


# конфигурируем логгер


async def update_last_message_number(message_number):
    cur.execute("UPDATE \"LastMessage\" SET \"MessageNumber\" = %s WHERE \"Id\" = 1", (message_number,))
    conn.commit()


def is_merchant_visible(provider_id):
    cur.execute("SELECT \"Id\" FROM \"GatewayMerchants\" WHERE \"ProviderId\"=%s", (provider_id,))
    gateway_merchant_id = cur.fetchone()[0]
    cur.execute("SELECT \"IsEnable\" FROM \"Merchants\" WHERE \"GatewayMerchantId\"=%s", (gateway_merchant_id,))
    is_enable = cur.fetchone()[0]
    return is_enable


def set_merchant_visibility(provider_id, is_enable):
    cur.execute("SELECT \"Id\" FROM \"GatewayMerchants\" WHERE \"ProviderId\"=%s", (provider_id,))
    gateway_merchant_id = cur.fetchone()[0]
    cur.execute("UPDATE \"Merchants\" SET \"IsEnable\"=%s WHERE \"GatewayMerchantId\"=%s", (is_enable, gateway_merchant_id,))
    conn.commit()


# создаем Telegram-клиента
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


@client.on(events.NewMessage(chats=1001441051007))
async def handle_message(event):
    pattern = r"^\s*PAYNET\s+(ОТКЛЮЧИЛ|ВКЛЮЧИЛ)\s+провайдера\s*:\s*(\d+)\s*:\s*((\S+\s?)+)?\s*$"

    # pattern = r"^\s*PAYNET\s+(ОТКЛЮЧИЛ|ВКЛЮЧИЛ)\s+провайдера\s*:\s*(\d+)\s*:\s*(\S+)?\s*$"
    m = re.search(pattern, event.message.message, flags=re.UNICODE)
    print(m)
    if m:
        action = m.group(1)
        provider_id = m.group(2)
        is_enable = (action == "ВКЛЮЧИЛ")
        set_merchant_visibility(provider_id, is_enable)
        message = f"Merchant {provider_id} {'enabled' if is_enable else 'disabled'}"
        logger.info(message)

        # обновляем айди последнего сообщения в базе данных
        await update_last_message_number(event.message.id)


if __name__ == "__main__":
    client.start()
    logger.info("Service is started!")
    client.run_until_disconnected()



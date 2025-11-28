import time
import json
import random
import uuid
import psycopg2
from kafka import KafkaProducer
from faker import Faker

# إعدادات الاتصال
PG_CONFIG = {
    "host": "postgres",
    "database": "app_db",
    "user": "user",
    "password": "password"
}

KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
TOPIC_NAME = 'engagement_events'

fake = Faker()

def get_pg_connection():
    """محاولة الاتصال بقاعدة البيانات حتى تنجح"""
    while True:
        try:
            conn = psycopg2.connect(**PG_CONFIG)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError:
            print("\u23f3 Waiting for Postgres to start...")
            time.sleep(2)

def get_kafka_producer():
    """محاولة الاتصال بـ Kafka حتى تنجح"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            return producer
        except Exception:
            print("\u23f3 Waiting for Kafka to start...")
            time.sleep(2)

def init_db(conn):
    """إنشاء الجداول في قاعدة البيانات"""
    commands = [
        """
        CREATE TABLE IF NOT EXISTS content (
            id UUID PRIMARY KEY,
            slug TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            content_type TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
            length_seconds INTEGER,
            publish_ts TIMESTAMPTZ NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS engagement_events (
            id BIGSERIAL PRIMARY KEY,
            content_id UUID REFERENCES content(id),
            user_id UUID,
            event_type TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
            event_ts TIMESTAMPTZ NOT NULL,
            duration_ms INTEGER,
            device TEXT,
            raw_payload JSONB
        )
        """
    ]
    with conn.cursor() as cur:
        for command in commands:
            cur.execute(command)
    print("\u2705 Database tables initialized.")

def generate_content(conn, num_items=50):
    """توليد محتوى أولي"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM content")
        if cur.fetchone()[0] > 0:
            print("\u2139\ufe0f Content already exists.")
            return

        print(f"\U0001f680 Generating {num_items} content items...")
        for _ in range(num_items):
            cur.execute("""
                INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (
                str(uuid.uuid4()),
                fake.slug(),
                fake.sentence(nb_words=4),
                random.choice(['video', 'podcast', 'newsletter']),
                random.randint(60, 3600)
            ))

def stream_events(conn, producer):
    """حلقة لا نهائية لتوليد الأحداث"""
    with conn.cursor() as cur:
        cur.execute("SELECT id, length_seconds FROM content")
        contents = cur.fetchall()

    print("\U0001f525 Starting Event Stream...")

    while True:
        try:
            content_id, length_sec = random.choice(contents)
            event_type = random.choice(['play', 'pause', 'finish', 'click'])

            duration_ms = None
            if event_type in ['play', 'finish']:
                duration_ms = random.randint(1000, length_sec * 1000)

            event_data = {
                "content_id": content_id,
                "user_id": str(uuid.uuid4()),
                "event_type": event_type,
                "event_ts": time.strftime('%Y-%m-%d %H:%M:%S'),
                "duration_ms": duration_ms,
                "device": random.choice(['ios', 'android', 'web']),
                "raw_payload": {"meta": "simulated"}
            }

            # 1. إدخال في Postgres
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO engagement_events
                    (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    event_data['content_id'], event_data['user_id'], event_data['event_type'],
                    event_data['event_ts'], event_data['duration_ms'], event_data['device'],
                    json.dumps(event_data['raw_payload'])
                ))

            # 2. إرسال إلى Kafka
            producer.send(TOPIC_NAME, value=event_data)

            print(f"\U0001f4e1 Sent: {event_type} -> {content_id}")
            time.sleep(0.5)

        except Exception as e:
            print(f"\u274c Error: {e}")

if __name__ == "__main__":
    pg_conn = get_pg_connection()
    kafka_prod = get_kafka_producer()

    init_db(pg_conn)
    generate_content(pg_conn)
    stream_events(pg_conn, kafka_prod)

FROM python:3.9-slim

# تثبيت Java (مطلوب لـ Spark) وأدوات النظام
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl procps && \
    apt-get clean;

# إعداد مجلد العمل
WORKDIR /app

# نسخ ملف المتطلبات وتثبيتها
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ باقي ملفات المشروع
COPY . .

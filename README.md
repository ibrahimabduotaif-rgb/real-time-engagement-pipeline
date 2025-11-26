# مستند تصميم نظام بث وتحليل تفاعل المستخدمين (Real‑time Engagement Pipeline)

## نظرة عامة (Overview)
يهدف هذا المشروع إلى بناء أنبوب بيانات (Data Pipeline) متين وقابل للتوسع لبث أحداث تفاعل المستخدمين من قاعدة بيانات علائقية (PostgreSQL)، ومعالجتها في الوقت الفعلي، وتوزيعها على وجهات متعددة لأغراض التحليل (BigQuery)، والتفاعل اللحظي (Redis)، والتكامل مع الأنظمة الخارجية.

## المعمارية المقترحة (System Architecture)
لتحقيق متطلبات "زمن الاستجابة المنخفض" (< 5 ثوانٍ) و"الدقة العالية" (Exactly‑once)، تم اعتماد معمارية تعتمد على الأحداث (Event‑Driven Architecture) باستخدام المكونات التالية:

1. **المصدر (Source):** PostgreSQL مع تفعيل WAL logs لمحاكاة تغيير البيانات (CDC).
2. **طبقة الاستيعاب (Ingestion Layer):** Apache Kafka يعمل كوسيط (Buffer) لفصل الأنظمة وضمان عدم فقدان البيانات (Backpressure Handling).
3. **محرك المعالجة (Stream Processing Engine):** Apache Spark Structured Streaming. تم اختياره لقدرته العالية على التعامل مع البيانات الضخمة، دعم النوافذ الزمنية (Windowing)، والتكامل القوي مع مصادر ومصبات متعددة.
4. **الوجهات (Sinks):**
   * **Redis:** لتخزين القوائم المتصدرة (Leaderboards) باستخدام هياكل البيانات `Sorted Sets`.
   * **Google BigQuery:** (محاكاة عبر ملفات Parquet/JSON) للتخزين التحليلي.
   * **External System:** (محاكاة HTTP API) للتكامل الخارجي.

## القرارات التقنية (Technology Decisions & Rationale)

| المكون | التقنية المختارة | سبب الاختيار |
|---|---|---|
| **Ingestion** | **Kafka + Producer** | يوفر Kafka متانة عالية (Durability) ويمكّن من إعادة معالجة البيانات (Replay/Backfill) ببساطة عبر تغيير الـ Offset. |
| **Processing** | **Spark Structured Streaming** | يوفر تجريداً عالياً (DataFrame API) للقيام بعمليات الـ JOIN والتحويلات المعقدة، مع ضمان تحمل الأعطال (Fault‑tolerance) وإدارة الحالة (State Management). |
| **Real‑time DB** | **Redis (Sorted Sets)** | هيكل البيانات `ZSET` في Redis هو الحل الأمثل لحساب "أكثر المحتويات تفاعلاً" بسرعة O(log(N)) مما يحقق شرط الـ 5 ثوانٍ. |
| **Environment** | **Docker Compose** | لضمان بيئة تشغيل موحدة (Reproducibility) وسهولة النشر والتشغيل لدى المراجعين. |

## استراتيجية البيانات (Data Strategy)

### التحويل والإثراء (Transformation Logic)
يتم تحميل جدول `content` كإطار بيانات ثابت أو يتم تحديثه دورياً، ويتم عمل `JOIN` مع تدفق `engagement_events` بناءً على `content_id`.

**المعادلات المطبقة:**

1. `engagement_seconds = duration_ms / 1000.0`  
2. `engagement_pct = engagement_seconds / length_seconds`  
   * عند غياب `length_seconds` أو `duration_ms` تُعاد القيمة `NULL` ويُستخدم `coalesce` أو `when` للتعامل مع القيم الفارغة وتجنّب أخطاء القسمة.

### استراتيجية التجميع (Aggregation Strategy for Redis)

* **الهدف:** معرفة المحتوى الأكثر تفاعلاً في آخر 10 دقائق.
* **التقنية:** نوافذ منزلقة (Sliding Windows).
* **التنفيذ:** تجميع البيانات وحساب `count` أو `sum(engagement_seconds)` لكل نافذة بطول 10 دقائق وبفاصل زمني صغير، ثم كتابة النتيجة في Redis مع ضبط TTL لضمان حداثة النتائج.

### التعامل مع البيانات التاريخية (Backfilling)

بفضل استخدام **Kafka**، يمكن إعادة معالجة البيانات التاريخية بسهولة عن طريق توجيه الـ Spark Job لقراءة البيانات من `earliest offset` بدلاً من `latest`. تم تصميم النظام ليكون **Idempotent** بحيث لا يؤدي تكرار المعالجة إلى فساد النتائج في Redis.

## دليل التشغيل (Setup Instructions)

يتطلب التشغيل وجود **Docker** و **Docker Compose**. لتنفيذ النظام:

1. **تشغيل البنية التحتية:**
   ```bash
   docker-compose up -d
   ```
2. **تهيئة قاعدة البيانات وتوليد البيانات:**
   ```bash
   docker exec -it spark-master python /app/src/generator.py
   ```
   * يقوم هذا السكربت بإنشاء الجداول في Postgres، توليد بيانات `content`، وضخ أحداث `engagement_events` إلى Kafka.
3. **تشغيل معالج البيانات (Stream Processor):**
   ```bash
   docker exec -it spark-master spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,redis.clients:jedis:4.3.1 \
      /app/src/processor.py
   ```
4. **التحقق من النتائج:**
   * **Redis:**
     ```bash
     docker exec -it redis redis-cli ZRANGE top_content 0 -1 WITHSCORES
     ```
   * **BigQuery – Local:** تفقد المجلد:
     ```bash
     ./data/bigquery_output
     ```

## الكود المصدري (Code Implementation)

يتضمن المشروع ملفات رئيسية تشرح كيفية تنفيذ الحل:

1. **processor.py** – برنامج بث البيانات باستخدام Spark Structured Streaming، يقوم بقراءة البيانات من Kafka، إجراء join مع جدول `content`, حساب المؤشرات (`engagement_seconds`, `engagement_pct`)، والكتابة إلى BigQuery وRedis ونظام خارجي.
2. **docker-compose.yml** – تعريف الخدمات (Kafka, Zookeeper, Postgres, Redis, Spark) لضمان تشغيل متكامل وسهل.

## التحسينات المستقبلية (Future Improvements)

1. **Schema Registry:** استخدام Confluent Schema Registry لإدارة مخططات Kafka بصيغ Avro أو Protobuf وتقليل حجم الرسائل وضمان التوافق بين المنتج والمستهلك.
2. **Dead Letter Queue (DLQ):** فصل الرسائل التالفة أو غير القابلة للمعالجة في طابور منفصل لفحصها لاحقاً بدلاً من إيقاف التدفق كله.
3. **CI/CD Pipeline:** أتمتة اختبارات الوحدات ونشر الكود باستخدام أدوات مثل GitHub Actions لضمان جودة ونشر سريع.
4. **Monitoring:** دمج Prometheus وGrafana لمراقبة تأخر المستهلك (Consumer Lag) ومعدل المعالجة، وتوفير لوحات متابعة لأداء النظام.
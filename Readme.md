# Crypto ETL Pipeline (CoinGecko → PostgreSQL)

ETL-пайплайн для получения курсов криптовалют из CoinGecko и загрузки данных в PostgreSQL.

Проект реализует полный цикл:

- Extract — получение топ-10 криптовалют через CoinGecko API  
- Transform — очистка, приведение типов, дедупликация  
- Load — идемпотентная загрузка в PostgreSQL  
- Data Mart — SQL-витрина с аналитикой за 24 часа  

---

## 🏗 Архитектура

CoinGecko API
|
v
Extract (Python + requests)
|
v
Transform (pandas)
|
v
PostgreSQL
├── stg_currency_rates (сырые данные)
├── fct_currency_rates (очищенные данные)
└── v_daily_crypto_stats (витрина)


---

## 📦 Стек технологий

- Python 3.11
- pandas
- requests
- SQLAlchemy + psycopg2
- PostgreSQL 15
- Docker + Docker Compose
- loguru (логирование)
- tenacity (retry API)

---

## 📊 Структура БД

### stg_currency_rates (staging)

Сырые данные из CoinGecko.

Уникальный ключ:(snapshot_ts, coin_id)


Хранится полный JSON ответа API.

---

### fct_currency_rates (fact)

Очищенная таблица:

- snapshot_ts
- coin_id
- symbol
- price
- market_cap
- volume_24h

Идемпотентная загрузка через:


---

### v_daily_crypto_stats (витрина)

SQL View рассчитывает за последние 24 часа:

- среднюю цену
- минимальную цену
- максимальную цену
- последнюю цену
- отклонение текущей цены от среднего (%)

Все агрегации выполняются средствами PostgreSQL с использованием оконных функций.

---

## ⚙️ Переменные окружения

Создай `.env`:

```bash
cp .env.example .env

POSTGRES_DB=demo_db
POSTGRES_USER=demo_db
POSTGRES_PASSWORD=demo_password
POSTGRES_HOST=db
POSTGRES_PORT=5432

COINGECKO_URL=https://api.coingecko.com/api/v3/coins/markets
VS_CURRENCY=usd
TOP_N=10
REQUEST_TIMEOUT=20


Запуск проекта

docker compose up --build


При запуске:

1.Поднимается PostgreSQL

2.Создаются таблицы, индексы и view

3.Запускается ETL

4.Загружаются топ-10 криптовалют

etl/
    app/
    ├── __init__.py
    ├── extract.py
    ├── transform.py
    ├── load.py
    ├── db.py
    ├── config.py
    └── main.py
    Dockerfile
    requirments.txt

docker-compose.yml
.env.example

db/
  init/
    ├── 001_schemas.sql
    ├── 002_indexes.sql
    └── 003_views.sql


✅ Реализовано
Extract

 CoinGecko /coins/markets

 retry с exponential backoff

 логирование запросов

 обработка временных ошибок API

Transform

 приведение типов

 фильтрация некорректных данных (price <= 0)

 дедупликация (snapshot_ts, coin_id)

 нормализация символов

Load

 staging + fact слой

 INSERT ... ON CONFLICT

 идемпотентная загрузка

 безопасные повторные запуски
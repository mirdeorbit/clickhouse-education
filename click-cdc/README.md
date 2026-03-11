# ClickHouse CDC Project

## Overview

ClickHouse Change Data Capture (CDC) проект, реализующий потоковую передачу изменений данных из PostgreSQL в ClickHouse через Debezium и Kafka.

### Версия: 1.0.345

## Архитектура

Проект состоит из трёх основных компонентов:

1. **PostgreSQL** - исходная база данных для данных заказов и доставки
2. **Debezium** - CDC коннектор для захвата изменений из PostgreSQL
3. **ClickHouse** - целевая аналитическая база данных

### Поток данных

`PostgreSQL` → `Debezium` → `Kafka` → `ClickHouse`

## Структура базы данных ClickHouse

### Базы данных:
- `cdc` - база для приёма CDC данных из Kafka
- `clients` - клиентские данные с материализованными представлениями
- `ods` (Operational Data Store) - подготовленные данные для аналитики
- `metrics` - метрики и агрегации

### Основные таблицы:
- `delivery_polygons` - полигоны доставки
- `delivery_shops` - магазины
- `delivery_couriers` - курьеры
- `delivery_pickers` - сборщики заказов
- `delivery_products` - товары
- `delivery_clients` - клиенты
- `delivery_orders` - заказы
- `delivery_order_products` - позиции заказов
- `delivery_work_shifts` - смены курьеров

## Описание таблиц и стратегии партиционирования

### База данных `cdc` (CDC слой)

Таблицы в этой базе данных принимают сырые изменения из Kafka в формате Debezium.

**Партиционирование**: `toYYYYMMDD(toDateTime(ts_ms / 1e6))` - по дням
- Позволяет удалять старые данные целыми партициями через TTL
- Оптимально для append-only workload CDC событий
- Хранение данных ограничено TTL на 7 дней

**ORDER BY**: `ts_ms`
- Сообщения приходят в порядке времени, что минимизирует пересортировки
- Улучшает производительность для append паттернов
- Поддерживает хронологический анализ изменений

**Таблицы CDC**:
- `postgres_delivery_public_polygons` - изменения полигонов
- `postgres_delivery_public_shops` - изменения магазинов
- `postgres_delivery_public_couriers` - изменения курьеров
- `postgres_delivery_public_pickers` - изменения сборщиков
- `postgres_delivery_public_products` - изменения товаров
- `postgres_delivery_public_clients` - изменения клиентов
- `postgres_delivery_public_orders` - изменения заказов
- `postgres_delivery_public_order_products` - изменения позиций заказов
- `postgres_delivery_public_work_shifts` - изменения смен

### База данных `clients` (Слой событий)

Таблицы с двигателем `Kafka` для стриминга событий заказчику без партиционирования.

**ORDER BY**: Не применимо для Kafka engine
- Dанные транзитно проходят через этот слой
- Используются материализованные представления для фильтрации только изменённых полей
- `diff_value` функция определяет, какие поля действительно изменились

**Таблицы clients**:
- `delivery_*_events` - таблицы для стриминга изменений заказчику

### База данных `ods` (Operational Data Store)

Предназначена для аналитических запросов и JOIN операций. Использует `ReplicatedReplacingMergeTree` для хранения актуальной версии данных.

#### Справочные таблицы (словари)

**Партиционирование**: отсутствует
- Небольшой объём данных (тысячи записей)
- Поиск по диапазонам дат не требуется
- Упрощает структуру и обслуживание

**ORDER BY**: `id`
- Самый частый паттерн - поиск по первичному ключу
- Оптимизирует JOIN операции
- Обеспечивает уникальность и быстрый доступ

**Таблицы справочников**:
- `delivery_polygons` - полигоны (ORDER BY id)
- `delivery_shops` - магазины (ORDER BY id)
- `delivery_couriers` - курьеры (ORDER BY id)
- `delivery_pickers` - сборщики (ORDER BY id)
- `delivery_products` - товары (ORDER BY id, ~5 тысяч товаров)
- `delivery_order_products` - позиции заказов (ORDER BY (order_id, product_id))

#### Таблицы с партиционированием по времени

**Таблица**: `delivery_clients`
- **PARTITION BY**: `toYYYYMM(created_at)`
- **ORDER BY**: `id`
- **Обоснование**: Аналитические запросы по периодам регистрации клиентов

**Таблица**: `delivery_orders`
- **PARTITION BY**: `toYYYYMM(create_date)`
- **ORDER BY**: `(create_date, shop_id, status, id)`
- **Обоснование**: 
  - Статистика заказов в разрезе времени (основной паттерн)
  - Фильтрация по магазину, статусу и ID (дополнительные фильтры)
  - Композитный ORDER BY оптимизирует многомерные фильтры

**Таблица**: `delivery_work_shifts`
- **PARTITION BY**: `toYYYYMM(start_date)`
- **ORDER BY**: `(start_date, courier_id)`
- **Обоснование**:
  - Основной паттерн: `WHERE start_date BETWEEN ... AND ... AND courier_id = ?`
  - Композитный ключ оптимизирует совместную фильтрацию по дате и курьеру

### База данных `metrics` (Метрики и агрегации)

Предназначена для быстрой аналитики производительности.

**Таблица**: `order_collecting_time`
- **PARTITION BY**: `toYYYYMM(toDateTime(collecting_end_date))`
- **ORDER BY**: `(city, collecting_end_date, order_id)`
- **Обоснование**:
  - Партиционирование по месяцам для долгосрочного хранения и удаления старых данных
  - Группа по городам для региональной аналитики
  - Время завершения для хронологического анализа
  - ID для точечных запросов

**Таблица**: `order_delivery_time`
- **PARTITION BY**: `toYYYYMM(toDateTime(courier_delivered_date))`
- **ORDER BY**: `(city, courier_delivered_date, order_id)`
- **Обоснование**:
  - Аналогично заказам с фокусом на время доставки
  - Группа по городам для сравнения метрик между регионами
  - Хронологический порядок для трендового анализа

## Развертывание

### Требования:
- Kubernetes кластер (Minikube / Kind / и др.)
- Helm 3.12+
- kubectl

### Запуск всех сервисов:

```bash
cd scripts
./deploy.sh
```

### Установка отдельных компонентов:

```bash
# PostgreSQL
./deploy.sh postgres

# ClickHouse с CDC
./deploy.sh click-cdc

# Debezium с Kafka
./deploy.sh debezium
```

### Удаление:

```bash
# Удалить все сервисы
./deploy.sh uninstall

# Удалить определённый сервис
./deploy.sh postgres uninstall
./deploy.sh click-cdc uninstall
./deploy.sh debezium uninstall
```

## Структура проекта

```
click-cdc/
├── config/                 # Конфигурационные файлы
│   ├── kube/              # Kubernetes конфиги
│   └── postgres/          # PostgreSQL конфиги
├── scripts/               # Скрипты деплоя和管理
│   ├── deploy.sh          # Основной скрипт деплоя
│   ├── install.sh         # Установка Helm chartов
│   ├── drop.sh            # Удаление сервисов
│   ├── test-data/         # Скрипты для тестовых данных
│   └── images/            # Docker образы
├── templates/             # Helm charts
│   ├── postgres/          # PostgreSQL chart
│   ├── click-cdc/         # ClickHouse chart
│   │   └── sql/          # SQL скрипты инициализации
│   └── debezium/          # Debezium chart
├── values/                # Helm values
│   ├── postgres/          # PostgreSQL конфиги
│   ├── click-cdc/         # ClickHouse конфиги
│   └── debezium/          # Debezium конфиги
└── version.txt            # Версия проекта
```

## Тестирование

Создание тестовых данных:

```bash
cd scripts/test-data
./create_test_data.sh
```

Выполнение тестовых запросов:

```bash
# PostgreSQL
./exec_postgres.sh

# ClickHouse
./exec_click.sh
```

## Технологии

- **PostgreSQL**: 18.2.6
- **ClickHouse**: 25.7.5
- **Debezium**: 4.1.1
- **Strimzi Kafka Operator**: 0.51.0
- **Kubernetes**: 1.26+

## Конфигурация

Конфигурационные файлы расположены в директориях `values/*/config.yaml`

Основные параметры:
- Репликации ClickHouse: 2
- Репликации Kafka: 3
- Репликации Zookeeper: 3
- Memory limits: ClickHouse 4Gi, другие компоненты 1-2Gi

## Полезные команды

Порт-форвардинг для доступа к сервисам:

```bash
# PostgreSQL
./scripts/pg_port_forward.sh
```

Проверка статус подов:

```bash
kubectl get pods
kubectl get svc
```

## Поддержка

Для создания тестовых данных используется JavaScript конфигурация в `scripts/test-data/seeds/`
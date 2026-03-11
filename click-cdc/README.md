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
- `clients` - данные о клиентах
- `orders` - заказы
- `delivery` - данные о доставке

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
CREATE DATABASE IF NOT EXISTS metrics
COMMENT 'База данных, в которой сформированы таблицы (для примера) двух метрик - время сборки заказа и время доставки';

-- Партиционирование по месяцам на основе времени CDC события.
-- Порядок сортировки: город - время завершения сборки - ID заказа для оптимизации фильтрации и аналитических запросов
CREATE TABLE IF NOT EXISTS metrics.order_collecting_time
(
    order_id UInt64 COMMENT 'ID заказа',
    picker_id UInt64 COMMENT 'ID сборщика, который собирал заказ',
    city String COMMENT 'Город заказа',

    collecting_start_date DateTime COMMENT 'Время начала сборки заказа',
    collecting_end_date DateTime COMMENT 'Время окончания сборки заказа',

    collecting_time_sec UInt32 COMMENT 'Длительность сборки заказа в секундах (collecting_end - collecting_start)',
    ts_ms UInt64 COMMENT 'Время события CDC, когда сборка была завершена'
)
ENGINE = ReplicatedMergeTree('/metrics/tables/{shard}/{database}/order_collecting_time', 'replica_{replica}')
PARTITION BY toYYYYMM(toDateTime(collecting_end_date))
ORDER BY (city, collecting_end_date, order_id);


CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.mv_collecting_time
TO metrics.order_collecting_time
AS
SELECT
    `after.id` AS order_id,
    `after.picker_id` AS picker_id,
    `after.city` AS city,

    toDateTime64(`after.collecting_start_date` / 1e6, 3) AS collecting_start_date,
    toDateTime64(`after.collecting_end_date` / 1e6, 3) AS collecting_end_date,

    (`after.collecting_end_date` - `after.collecting_start_date`) / 1e6 AS collecting_time_sec,
    ts_ms
FROM cdc.postgres_delivery_public_orders
WHERE
    op = 'u'
    AND `before.collecting_end_date` IS NULL
    AND `after.collecting_end_date` IS NOT NULL;


----------------------------------------------------------------------------------------------------
-- Партиционирование по месяцам на основе времени CDC события.
-- Порядок сортировки: город - время завершения доставки - ID заказа для оптимизации фильтрации и аналитических запросов
CREATE TABLE IF NOT EXISTS metrics.order_delivery_time
(
    order_id UInt64 COMMENT 'ID заказа',
    courier_id UInt64 COMMENT 'ID курьера, который доставил заказ',
    city String COMMENT 'Город заказа',

    courier_take_date DateTime COMMENT 'Время взятия заказа курьером',
    courier_delivered_date DateTime COMMENT 'Время доставки заказа',

    delivery_time_sec UInt32 COMMENT 'Длительность доставки заказа в секундах (courier_delivered - courier_take)',
    ts_ms UInt64 COMMENT 'Время события CDC, когда доставка была завершена'
)
ENGINE = ReplicatedMergeTree('/metrics/tables/{shard}/{database}/order_delivery_time', 'replica_{replica}')
PARTITION BY toYYYYMM(toDateTime(courier_delivered_date))
ORDER BY (city, courier_delivered_date, order_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS metrics.mv_delivery_time
TO metrics.order_delivery_time
AS
SELECT
    `after.id` AS order_id,
    `after.courier_id` AS courier_id,
    `after.city` AS city,

    toDateTime64(`after.courier_take_date` / 1e6, 3) AS courier_take_date,
    toDateTime64(`after.courier_delivered_date` / 1e6, 3) AS courier_delivered_date,

    (`after.courier_delivered_date` - `after.courier_take_date`) / 1e6 AS delivery_time_sec,
    ts_ms
FROM cdc.postgres_delivery_public_orders
WHERE
    op = 'u'
    AND `before.courier_delivered_date` IS NULL
    AND `after.courier_delivered_date` IS NOT NULL;
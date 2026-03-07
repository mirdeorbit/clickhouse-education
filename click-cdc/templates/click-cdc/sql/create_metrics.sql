CREATE DATABASE IF NOT EXISTS metrics
COMMENT 'ClickHouse metrics DataBase';

CREATE TABLE metrics.order_collecting_time
(
    order_id UInt64 COMMENT 'ID заказа',
    picker_id UInt64 COMMENT 'ID сборщика, который собирал заказ',
    city String COMMENT 'Город заказа',

    collecting_start UInt64 COMMENT 'Время начала сборки заказа',
    collecting_end UInt64 COMMENT 'Время окончания сборки заказа',

    collecting_time_sec UInt32 COMMENT 'Длительность сборки заказа в секундах (collecting_end - collecting_start)',
    ts_ms UInt64 COMMENT 'Время события CDC, когда сборка была завершена'
)
ENGINE = ReplicatedMergeTree('/metrics/tables/{shard}/{database}/order_collecting_time', 'replica_{replica}')
ORDER BY (order_id);


CREATE MATERIALIZED VIEW metrics.mv_collecting_time
TO metrics.order_collecting_time
AS
SELECT
    `after.id` AS order_id,
    `after.picker_id` AS picker_id,
    `after.city` AS city,

    `after.collecting_start_date` AS collecting_start,
    `after.collecting_end_date` AS collecting_end,

    (`after.collecting_end_date` - `after.collecting_start_date`) / 1e6 AS collecting_time_sec,
    ts_ms
FROM cdc.postgres_delivery_public_orders
WHERE
    op = 'u'
    AND `before.collecting_end_date` IS NULL
    AND `after.collecting_end_date` IS NOT NULL;


----------------------------------------------------------------------------------------------------

CREATE TABLE metrics.order_delivery_time
(
    order_id UInt64 COMMENT 'ID заказа',
    courier_id UInt64 COMMENT 'ID курьера, который доставил заказ',
    city String COMMENT 'Город заказа',

    courier_take UInt64 COMMENT 'Время взятия заказа курьером',
    courier_delivered UInt64 COMMENT 'Время доставки заказа',

    delivery_time_sec UInt32 COMMENT 'Длительность доставки заказа в секундах (courier_delivered - courier_take)',
    ts_ms UInt64 COMMENT 'Время события CDC, когда доставка была завершена'
)
ENGINE = ReplicatedMergeTree('/metrics/tables/{shard}/{database}/order_delivery_time', 'replica_{replica}')
ORDER BY (order_id);


CREATE MATERIALIZED VIEW metrics.mv_delivery_time
TO metrics.order_delivery_time
AS
SELECT
    `after.id` AS order_id,
    `after.courier_id` AS courier_id,
    `after.city` AS city,

    `after.courier_take_date` AS courier_take,
    `after.courier_delivered_date` AS courier_delivered,

    (`after.courier_delivered_date` - `after.courier_take_date`) / 1e6 AS delivery_time_sec,
    ts_ms
FROM cdc.postgres_delivery_public_orders
WHERE
    op = 'u'
    AND `before.courier_delivered_date` IS NULL
    AND `after.courier_delivered_date` IS NOT NULL;
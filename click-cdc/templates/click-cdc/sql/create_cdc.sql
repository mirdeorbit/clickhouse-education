CREATE DATABASE IF NOT EXISTS cdc
COMMENT 'ClickHouse CDC database';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_polygons
(
	`before.id` Nullable(UInt64),
	`before.name` Nullable(String),
	`before.city` Nullable(String),
	`before.is_active` Nullable(UInt8),
	`before.created_at` Nullable(UInt64),
	`after.id` Nullable(UInt64),
	`after.name` Nullable(String),
	`after.city` Nullable(String),
	`after.is_active` Nullable(UInt8),
	`after.created_at` Nullable(UInt64),
	`op` LowCardinality(String),
	`ts_ms` UInt64,
	`source.table` String,
	`source.db` String
)
ENGINE = ReplicatedMergeTree('/cdc/tables/{shard}/{database}/postgres_delivery_public_polygons', 'replica_{replica}')
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.polygons';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_shops
(
    `before.id` Nullable(UInt64),
    `before.address` Nullable(String),
    `before.delivery_enabled` Nullable(UInt8),
    `before.start_work_time` Nullable(String),
    `before.end_work_time` Nullable(String),
    `before.city` Nullable(String),
    `before.polygon_id` Nullable(UInt64),
    `before.created_at` Nullable(UInt64),

    `after.id` Nullable(UInt64),
    `after.address` Nullable(String),
    `after.delivery_enabled` Nullable(UInt8),
    `after.start_work_time` Nullable(String),
    `after.end_work_time` Nullable(String),
    `after.city` Nullable(String),
    `after.polygon_id` Nullable(UInt64),
    `after.created_at` Nullable(UInt64),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_shops',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.shops';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_couriers
(
    `before.id` Nullable(UInt64),
    `before.first_name` Nullable(String),
    `before.last_name` Nullable(String),
    `before.patronymic` Nullable(String),
    `before.phone` Nullable(String),
    `before.email` Nullable(String),
    `before.inn` Nullable(String),
    `before.city` Nullable(String),
    `before.status` Nullable(String),
    `before.company` Nullable(String),
    `before.self_employed` Nullable(UInt8),
    `before.timezone` Nullable(String),
    `before.polygon_id` Nullable(UInt64),
    `before.created_at` Nullable(UInt64),

    `after.id` Nullable(UInt64),
    `after.first_name` Nullable(String),
    `after.last_name` Nullable(String),
    `after.patronymic` Nullable(String),
    `after.phone` Nullable(String),
    `after.email` Nullable(String),
    `after.inn` Nullable(String),
    `after.city` Nullable(String),
    `after.status` Nullable(String),
    `after.company` Nullable(String),
    `after.self_employed` Nullable(UInt8),
    `after.timezone` Nullable(String),
    `after.polygon_id` Nullable(UInt64),
    `after.created_at` Nullable(UInt64),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_couriers',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.couriers';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_pickers
(
    `before.id` Nullable(UInt64),
    `before.first_name` Nullable(String),
    `before.last_name` Nullable(String),
    `before.patronymic` Nullable(String),
    `before.phone` Nullable(String),
    `before.email` Nullable(String),
    `before.status` Nullable(String),
    `before.network` Nullable(String),
    `before.city` Nullable(String),
    `before.timezone` Nullable(String),
    `before.shop_id` Nullable(UInt64),
    `before.created_at` Nullable(UInt64),

    `after.id` Nullable(UInt64),
    `after.first_name` Nullable(String),
    `after.last_name` Nullable(String),
    `after.patronymic` Nullable(String),
    `after.phone` Nullable(String),
    `after.email` Nullable(String),
    `after.status` Nullable(String),
    `after.network` Nullable(String),
    `after.city` Nullable(String),
    `after.timezone` Nullable(String),
    `after.shop_id` Nullable(UInt64),
    `after.created_at` Nullable(UInt64),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_pickers',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.pickers';

CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_products
(
    `before.id` Nullable(UInt64),
    `before.title` Nullable(String),
    `before.amount_type` Nullable(String),
    `before.price_for_amount_item` Nullable(String),
    `before.valid_hours` Nullable(Int32),
    `before.total_amount` Nullable(String),
    `before.discount_percent` Nullable(Int32),
    `before.created_at` Nullable(UInt64),

    `after.id` Nullable(UInt64),
    `after.title` Nullable(String),
    `after.amount_type` Nullable(String),
    `after.price_for_amount_item` Nullable(String),
    `after.valid_hours` Nullable(Int32),
    `after.total_amount` Nullable(String),
    `after.discount_percent` Nullable(Int32),
    `after.created_at` Nullable(UInt64),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_products',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.products';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_clients
(
    `before.id` Nullable(UInt64),
    `before.full_name` Nullable(String),
    `before.phone` Nullable(String),
    `before.address` Nullable(String),
    `before.created_at` Nullable(UInt64),

    `after.id` Nullable(UInt64),
    `after.full_name` Nullable(String),
    `after.phone` Nullable(String),
    `after.address` Nullable(String),
    `after.created_at` Nullable(UInt64),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_clients',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.clients';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_orders
(
    `before.id` Nullable(UInt64),
    `before.city` Nullable(String),
    `before.status` Nullable(String),
    `before.shop_id` Nullable(UInt64),
    `before.client_id` Nullable(UInt64),
    `before.picker_id` Nullable(UInt64),
    `before.courier_id` Nullable(UInt64),
    `before.payment` Nullable(UInt8),

    `before.create_date` Nullable(UInt64),
    `before.pay_date` Nullable(UInt64),
    `before.collecting_start_date` Nullable(UInt64),
    `before.collecting_end_date` Nullable(UInt64),
    `before.courier_assigned_date` Nullable(UInt64),
    `before.courier_take_date` Nullable(UInt64),
    `before.courier_delivered_date` Nullable(UInt64),
    `before.completed_date` Nullable(UInt64),

    `after.id` Nullable(UInt64),
    `after.city` Nullable(String),
    `after.status` Nullable(String),
    `after.shop_id` Nullable(UInt64),
    `after.client_id` Nullable(UInt64),
    `after.picker_id` Nullable(UInt64),
    `after.courier_id` Nullable(UInt64),
    `after.payment` Nullable(UInt8),

    `after.create_date` Nullable(UInt64),
    `after.pay_date` Nullable(UInt64),
    `after.collecting_start_date` Nullable(UInt64),
    `after.collecting_end_date` Nullable(UInt64),
    `after.courier_assigned_date` Nullable(UInt64),
    `after.courier_take_date` Nullable(UInt64),
    `after.courier_delivered_date` Nullable(UInt64),
    `after.completed_date` Nullable(UInt64),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_orders',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.orders';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_order_products
(
    `before.order_id` Nullable(UInt64),
    `before.product_id` Nullable(UInt64),
    `before.amount` Nullable(String),
    `before.price` Nullable(String),

    `after.order_id` Nullable(UInt64),
    `after.product_id` Nullable(UInt64),
    `after.amount` Nullable(String),
    `after.price` Nullable(String),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_order_products',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.order_products';


CREATE TABLE IF NOT EXISTS cdc.postgres_delivery_public_work_shifts
(
    `before.id` Nullable(UInt64),
    `before.start_date` Nullable(UInt64),
    `before.end_date` Nullable(UInt64),
    `before.close_date` Nullable(UInt64),
    `before.courier_id` Nullable(UInt64),
    `before.status` Nullable(String),
    `before.close_reason` Nullable(String),
    `before.created_at` Nullable(UInt64),

    `after.id` Nullable(UInt64),
    `after.start_date` Nullable(UInt64),
    `after.end_date` Nullable(UInt64),
    `after.close_date` Nullable(UInt64),
    `after.courier_id` Nullable(UInt64),
    `after.status` Nullable(String),
    `after.close_reason` Nullable(String),
    `after.created_at` Nullable(UInt64),

    `op` LowCardinality(String),
    `ts_ms` UInt64,
    `source.table` String,
    `source.db` String
)
ENGINE = ReplicatedMergeTree(
    '/cdc/tables/{shard}/{database}/postgres_delivery_public_work_shifts',
    'replica_{replica}'
)
ORDER BY tuple()
COMMENT 'CDC source data table for topic postgres.public.work_shifts';
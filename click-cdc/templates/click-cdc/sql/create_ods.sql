CREATE DATABASE IF NOT EXISTS ods
COMMENT 'ClickHouse ODS (Operational Data Store) база данных для витрин';

CREATE TABLE IF NOT EXISTS ods.delivery_polygons
(
    id UInt64 COMMENT 'ID полигона',
    name String COMMENT 'Название полигона доставки',
    city LowCardinality(String) COMMENT 'Город полигона',
    is_active UInt8 COMMENT 'Флаг активности полигона (1 — активен)',
    created_at DateTime64(3) COMMENT 'Дата создания полигона в системе',

    is_deleted UInt8 COMMENT 'Признак удаления записи (1 — удалена)',
    ts_ms UInt64 COMMENT 'Время события CDC (epoch ms)'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_polygons', 'replica_{replica}', ts_ms)
ORDER BY id -- для join
-- партиционирование не нужно, мало данных не будет поисков по диапазону
COMMENT 'ODS слой полигонов доставки';


CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_polygons
TO ods.delivery_polygons
AS
SELECT
    coalesce(`after.id`, `before.id`) AS id,
    coalesce(`after.name`, `before.name`) AS name,
    coalesce(`after.city`, `before.city`) AS city,
    coalesce(`after.is_active`, `before.is_active`) AS is_active,
    toDateTime64(coalesce(`after.created_at`, `before.created_at`) / 1e6, 3) AS created_at,
    op = 'd' AS is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_polygons;


CREATE TABLE IF NOT EXISTS ods.delivery_shops
(
    id UInt64 COMMENT 'ID магазина',
    address String COMMENT 'Адрес магазина',
    delivery_enabled UInt8 COMMENT 'Доступна ли доставка из магазина',
    start_work_time String COMMENT 'Время открытия магазина',
    end_work_time String COMMENT 'Время закрытия магазина',
    city LowCardinality(String) COMMENT 'Город магазина',
    polygon_id Nullable(UInt64) COMMENT 'Полигон доставки магазина',
    created_at DateTime64(3) COMMENT 'Дата создания магазина в системе',

    is_deleted UInt8 COMMENT 'Признак удаления записи',
    ts_ms UInt64 COMMENT 'Время CDC события'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_shops', 'replica_{replica}', ts_ms)
ORDER BY id -- самый частый посиск по id, для join
-- партиционирование не нужно, мало данных не будет поисков по диапазону
COMMENT 'ODS слой магазинов доставки';


CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_shops
TO ods.delivery_shops
AS
SELECT
    coalesce(`after.id`, `before.id`) as id,
    coalesce(`after.address`, `before.address`) as address,
    coalesce(`after.delivery_enabled`, `before.delivery_enabled`) as delivery_enabled,
    coalesce(`after.start_work_time`, `before.start_work_time`) as start_work_time,
    coalesce(`after.end_work_time`, `before.end_work_time`) as end_work_time,
    coalesce(`after.city`, `before.city`) as city,
    coalesce(`after.polygon_id`, `before.polygon_id`) as  polygon_id,
    toDateTime64(coalesce(`after.created_at`, `before.created_at`) / 1e6, 3) AS created_at,
    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_shops;

CREATE TABLE IF NOT EXISTS ods.delivery_couriers
(
    id UInt64 COMMENT 'ID курьера',
    first_name String COMMENT 'Имя курьера',
    last_name String COMMENT 'Фамилия курьера',
    patronymic Nullable(String) COMMENT 'Отчество курьера',
    phone String COMMENT 'Телефон курьера',
    email Nullable(String) COMMENT 'Email курьера',
    inn Nullable(String) COMMENT 'ИНН курьера',
    city LowCardinality(String) COMMENT 'Город работы курьера',
    status LowCardinality(String) COMMENT 'Статус курьера (blocked/on_work/not_on_work)',
    company Nullable(String) COMMENT 'Компания курьера',
    self_employed UInt8 COMMENT 'Самозанятый ли курьер',
    timezone LowCardinality(String) COMMENT 'Таймзона курьера',
    polygon_id Nullable(UInt64) COMMENT 'Полигон работы курьера',
    created_at DateTime64(3) COMMENT 'Дата создания курьера',

    is_deleted UInt8 COMMENT 'Soft delete флаг',
    ts_ms UInt64 COMMENT 'Время CDC события'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_couriers', 'replica_{replica}', ts_ms)
ORDER BY id -- Считаем, что ищут только по id, нужно для join
-- партиционирование не нужно, мало данных не будет поисков по диапазону
COMMENT 'ODS слой курьеров доставки';

CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_couriers
TO ods.delivery_couriers
AS
SELECT
    coalesce(`after.id`, `before.id`) as id,
    coalesce(`after.first_name`, `before.first_name`) as first_name,
    coalesce(`after.last_name`, `before.last_name`) as last_name,
    coalesce(`after.patronymic`, `before.patronymic`) as patronymic,
    coalesce(`after.phone`, `before.phone`) as phone,
    coalesce(`after.email`, `before.email`) as email,
    coalesce(`after.inn`, `before.inn`) as inn,
    coalesce(`after.city`, `before.city`) as city,
    coalesce(`after.status`, `before.status`) as status,
    coalesce(`after.company`, `before.company`) as company,
    coalesce(`after.self_employed`, `before.self_employed`) as self_employed,
    coalesce(`after.timezone`, `before.timezone`) as timezone,
    coalesce(`after.polygon_id`, `before.polygon_id`) as polygon_id,
    toDateTime64(coalesce(`after.created_at`, `before.created_at`) / 1e6, 3) AS created_at,
    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_couriers;


CREATE TABLE IF NOT EXISTS ods.delivery_pickers
(
    id UInt64 COMMENT 'ID сборщика заказов',
    first_name String COMMENT 'Имя сборщика',
    last_name String COMMENT 'Фамилия сборщика',
    patronymic Nullable(String) COMMENT 'Отчество',
    phone String COMMENT 'Телефон',
    email Nullable(String) COMMENT 'Email',
    status LowCardinality(String) COMMENT 'Статус (blocked/free/busy)',
    network String COMMENT 'Сеть работы сборщика',
    city LowCardinality(String) COMMENT 'Город',
    timezone LowCardinality(String) COMMENT 'Таймзона',
    shop_id Nullable(UInt64) COMMENT 'Магазин работы сборщика',
    created_at DateTime64(3) COMMENT 'Дата создания сборщика',

    is_deleted UInt8 COMMENT 'Soft delete',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_pickers', 'replica_{replica}', ts_ms)
ORDER BY id -- Считаем, что ищем только по id, для join
-- Партиционирование не нужно
COMMENT 'ODS слой сборщиков заказов';

CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_pickers
TO ods.delivery_pickers
AS
SELECT
    coalesce(`after.id`, `before.id`) as id,
    coalesce(`after.first_name`, `before.first_name`) as first_name,
    coalesce(`after.last_name`, `before.last_name`) as last_name,
    coalesce(`after.patronymic`, `before.patronymic`) as patronymic,
    coalesce(`after.phone`, `before.phone`) as phone,
    coalesce(`after.email`, `before.email`) as email,
    coalesce(`after.status`, `before.status`) as status,
    coalesce(`after.network`, `before.network`) as network,
    coalesce(`after.city`, `before.city`) as city,
    coalesce(`after.timezone`, `before.timezone`) as timezone,
    coalesce(`after.shop_id`, `before.shop_id`) as shop_id,
    toDateTime64(coalesce(`after.created_at`, `before.created_at`) / 1e6, 3) AS created_at,
    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_pickers;


CREATE TABLE IF NOT EXISTS ods.delivery_products
(
    id UInt64 COMMENT 'ID товара',
    title String COMMENT 'Название товара',
    amount_type String COMMENT 'Тип измерения (шт/вес)',
    price_for_amount_item Decimal(10,2) COMMENT 'Цена за единицу',
    valid_hours Int32 COMMENT 'Срок годности в часах',
    total_amount Decimal(10,3) COMMENT 'Общий доступный объём',
    discount_percent Int32 COMMENT 'Скидка в процентах',
    created_at DateTime64(3) COMMENT 'Дата создания товара',

    is_deleted UInt8 COMMENT 'Soft delete',
    ts_ms UInt64 COMMENT 'Время CDC',
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_products', 'replica_{replica}', ts_ms)
ORDER BY id -- Считаем, что ищут только по id
-- 5 тысяч наименований продуктов, партиционирование не нужно
COMMENT 'ODS слой товаров';


CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_products
TO ods.delivery_products
AS
SELECT
    coalesce(`after.id`, `before.id`) as id,
    coalesce(`after.title`, `before.title`) as title,
    coalesce(`after.amount_type`, `before.amount_type`) as amount_type,
    CAST(coalesce(`after.price_for_amount_item`, `before.price_for_amount_item`) as Decimal(10, 2)) as price_for_amount_item,
    coalesce(`after.valid_hours`, `before.valid_hours`) as valid_hours,
    CAST(coalesce(`after.total_amount`, `before.total_amount`) as Decimal(10, 3)) as total_amount,
    coalesce(`after.discount_percent`, `before.discount_percent`) as discount_percent,
    toDateTime64(coalesce(`after.created_at`, `before.created_at`) / 1e6, 3) AS created_at,
    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_products;


CREATE TABLE IF NOT EXISTS ods.delivery_clients
(
    id UInt64 COMMENT 'ID клиента',
    full_name String COMMENT 'ФИО клиента',
    phone String COMMENT 'Телефон клиента',
    address String COMMENT 'Адрес доставки',
    created_at DateTime64(3) COMMENT 'Дата создания клиента',

    is_deleted UInt8 COMMENT 'Soft delete',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_clients', 'replica_{replica}', ts_ms)
ORDER BY id -- Считаем, что ищут только по id, для join
PARTITION BY toYYYYMM(created_at) -- Для аналитики, например новые клиенты за период
COMMENT 'ODS слой клиентов доставки';

CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_clients
TO ods.delivery_clients
AS
SELECT
    coalesce(`after.id`, `before.id`) AS id,
    coalesce(`after.full_name`, `before.full_name`) as full_name,
    coalesce(`after.phone`, `before.phone`) AS phone,
    coalesce(`after.address`, `before.address`) as address,
    toDateTime64(coalesce(`after.created_at`, `before.created_at`) / 1e6, 3) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_clients;


CREATE TABLE IF NOT EXISTS ods.delivery_orders
(
    id UInt64 COMMENT 'ID заказа',
    city LowCardinality(String) COMMENT 'Город заказа',
    status LowCardinality(String) COMMENT 'Статус заказа',
    shop_id UInt64 COMMENT 'Магазин заказа',
    client_id UInt64 COMMENT 'Клиент заказа',
    picker_id Nullable(UInt64) COMMENT 'Сборщик заказа',
    courier_id Nullable(UInt64) COMMENT 'Курьер заказа',
    payment UInt8 COMMENT 'Флаг оплаты',

    create_date DateTime64(3) COMMENT 'Дата создания заказа',
    pay_date Nullable(DateTime64(3)) COMMENT 'Дата оплаты',
    collecting_start_date Nullable(DateTime64(3)) COMMENT 'Дата начала сборки',
    collecting_end_date Nullable(DateTime64(3)) COMMENT 'Дата окончания сборки',
    courier_assigned_date Nullable(DateTime64(3)) COMMENT 'Дата назначения курьера',
    courier_take_date Nullable(DateTime64(3)) COMMENT 'Дата получения курьером заказа',
    courier_delivered_date Nullable(DateTime64(3)) COMMENT 'Дата доставки заказа',
    completed_date Nullable(DateTime64(3)) COMMENT 'Дата завершения/закрытия заказа',

    is_deleted UInt8 COMMENT 'Soft delete',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_orders', 'replica_{replica}', ts_ms)
-- Заказчиков чаще всего индересует статистика по заказам в разрезе времени
-- Но также может быть применен фильтр по магазину, статусу и айдишнику
ORDER BY (create_date, shop_id, status, id)
PARTITION BY toYYYYMM(create_date)
COMMENT 'ODS слой заказов доставки';

CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_orders
TO ods.delivery_orders
AS
SELECT
    coalesce(`after.id`, `before.id`) AS id,
    coalesce(`after.city`, `before.city`) AS city,
    coalesce(`after.status`, `before.status`) AS status,
    coalesce(`after.shop_id`, `before.shop_id`) AS shop_id,
    coalesce(`after.client_id`, `before.client_id`) AS client_id,
    coalesce(`after.picker_id`, `before.picker_id`) AS picker_id,
    coalesce(`after.courier_id`, `before.courier_id`) AS courier_id,
    coalesce(`after.payment`, `before.payment`) AS payment,
    toDateTime64(coalesce(`after.create_date`, `before.create_date`) / 1e6, 3) AS create_date,
    toDateTime64(coalesce(`after.pay_date`, `before.pay_date`) / 1e6, 3) AS pay_date,
    toDateTime64(coalesce(`after.collecting_start_date`, `before.collecting_start_date`) / 1e6, 3) AS collecting_start_date,
    toDateTime64(coalesce(`after.collecting_end_date`, `before.collecting_end_date`) / 1e6, 3) AS collecting_end_date,
    toDateTime64(coalesce(`after.courier_assigned_date`, `before.courier_assigned_date`) / 1e6, 3) AS courier_assigned_date,
    toDateTime64(coalesce(`after.courier_take_date`, `before.courier_take_date`) / 1e6, 3) AS courier_take_date,
    toDateTime64(coalesce(`after.courier_delivered_date`, `before.courier_delivered_date`) / 1e6, 3) AS courier_delivered_date,
    toDateTime64(coalesce(`after.completed_date`, `before.completed_date`) / 1e6, 3) AS completed_date,
    op = 'd' AS is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_orders;

-- Основной паттерн чтения из таблицы SELECT * FROM delivery_order_products WHERE order_id = ?
CREATE TABLE IF NOT EXISTS ods.delivery_order_products
(
    order_id UInt64 COMMENT 'ID заказа',
    product_id UInt64 COMMENT 'ID товара',
    amount Decimal(10,3) COMMENT 'Количество товара',
    price Decimal(10,2) COMMENT 'Цена позиции',
    created_at DateTime64(3) COMMENT 'Дата создания соотношения заказ-продукт',

    is_deleted UInt8 COMMENT 'Soft delete',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_order_products', 'replica_{replica}', ts_ms)
-- партиционирование не нужно 
ORDER BY (order_id, product_id)
COMMENT 'ODS слой позиций заказов';

CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_order_products
TO ods.delivery_order_products
AS
SELECT
    coalesce(`after.order_id`, `before.order_id`) AS order_id,
    coalesce(`after.product_id`, `before.product_id`) AS product_id,
    CAST(coalesce(`after.amount`, `before.amount`) as Decimal(10, 3)) AS amount,
    CAST(coalesce(`after.price`, `before.price`) as Decimal(10, 2)) AS price,

    op = 'd' AS is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_order_products;


CREATE TABLE IF NOT EXISTS ods.delivery_work_shifts
(
    id UInt64 COMMENT 'ID смены',
    start_date DateTime64(3) COMMENT 'Начало смены',
    end_date Nullable(DateTime64(3)) COMMENT 'Конец смены',
    close_date Nullable(DateTime64(3)) COMMENT 'Дата закрытия смены',
    courier_id UInt64 COMMENT 'Курьер смены',
    status String COMMENT 'Статус смены',
    close_reason Nullable(String) COMMENT 'Причина закрытия',
    created_at DateTime64(3) COMMENT 'Дата создания смены',

    is_deleted UInt8 COMMENT 'Soft delete',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = ReplicatedReplacingMergeTree('/ods/tables/{shard}/{database}/delivery_work_shifts', 'replica_{replica}', ts_ms)
-- Самый частный запрос предполагаем в эту таблицу - это 
-- WHERE start_date BETWEEN '2026-03-01' AND '2026-03-31' AND courier_id = 123
PARTITION BY toYYYYMM(start_date)
ORDER BY (start_date, courier_id)
COMMENT 'ODS слой смен курьеров';


CREATE MATERIALIZED VIEW IF NOT EXISTS ods.mv_delivery_work_shifts
TO ods.delivery_work_shifts
AS
SELECT
    coalesce(`after.id`, `before.id`) AS id,
    toDateTime64(coalesce(`after.start_date`, `before.start_date`) / 1e6, 3) AS start_date,
    toDateTime64(coalesce(`after.end_date`, `before.end_date`) / 1e6, 3) AS end_date,
    toDateTime64(coalesce(`after.close_date`, `before.close_date`) / 1e6, 3) AS close_date,
    coalesce(`after.courier_id`, `before.courier_id`) AS courier_id,
    coalesce(`after.status`, `before.status`) AS status,
    coalesce(`after.close_reason`, `before.close_reason`) AS close_reason,
    toDateTime64(coalesce(`after.created_at`, `before.created_at`) / 1e6, 3) AS created_at,

    op = 'd' AS is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_work_shifts;

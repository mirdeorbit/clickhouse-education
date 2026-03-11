-- Материлизованные представления для таблиц KafkaEngine, которые будут отправлять только измененные значения данных клиентам
-- Клиенты - приложения системы доставки, которые читают из топиков кафки и им нужны изменения данных из таблиц delivery

CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_polygons
TO clients.delivery_polygons_events
AS
SELECT
    coalesce(after.id, before.id) AS id,
    diff_value(before.name, after.name, op) AS name,
    diff_value(before.city, after.city, op) AS city,
    diff_value(before.is_active, after.is_active, op) AS is_active,
    if(isNotNull(diff_value(before.created_at, after.created_at, op) as cr_at), toDateTime64(cr_at / 1e6, 3), NULL) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_polygons
WHERE op IN ('c', 'u', 'd');


----------------------------------------------------------------------------------------------------


CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_shops
TO clients.delivery_shops_events
AS
SELECT
    coalesce(after.id, before.id) AS id,

    diff_value(before.address, after.address, op) AS address,
    diff_value(before.delivery_enabled, after.delivery_enabled, op) AS delivery_enabled,
    diff_value(before.start_work_time, after.start_work_time, op) AS start_work_time,
    diff_value(before.end_work_time, after.end_work_time, op) AS end_work_time,
    diff_value(before.city, after.city, op) AS city,
    diff_value(before.polygon_id, after.polygon_id, op) AS polygon_id,
    if(isNotNull(diff_value(before.created_at, after.created_at, op) as cr_at), toDateTime64(cr_at / 1e6, 3), NULL) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_shops
WHERE op IN ('c','u', 'd');

----------------------------------------------------------------------------------------------------


CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_couriers
TO clients.delivery_couriers_events
AS
SELECT
    coalesce(after.id, before.id) AS id,

    diff_value(before.first_name, after.first_name, op) AS first_name,
    diff_value(before.last_name, after.last_name, op) AS last_name,
    diff_value(before.patronymic, after.patronymic, op) AS patronymic,
    diff_value(before.phone, after.phone, op) AS phone,
    diff_value(before.email, after.email, op) AS email,
    diff_value(before.inn, after.inn, op) AS inn,
    diff_value(before.city, after.city, op) AS city,
    diff_value(before.status, after.status, op) AS status,
    diff_value(before.company, after.company, op) AS company,
    diff_value(before.self_employed, after.self_employed, op) AS self_employed,
    diff_value(before.timezone, after.timezone, op) AS timezone,
    diff_value(before.polygon_id, after.polygon_id, op) AS polygon_id,

    if(
        isNotNull(diff_value(before.created_at, after.created_at, op) AS cr_at),
        toDateTime64(cr_at / 1e6, 3),
        NULL
    ) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_couriers
WHERE op IN ('c','u','d');

----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_pickers
TO clients.delivery_pickers_events
AS
SELECT
    coalesce(after.id, before.id) AS id,

    diff_value(before.first_name, after.first_name, op) AS first_name,
    diff_value(before.last_name, after.last_name, op) AS last_name,
    diff_value(before.patronymic, after.patronymic, op) AS patronymic,
    diff_value(before.phone, after.phone, op) AS phone,
    diff_value(before.email, after.email, op) AS email,
    diff_value(before.status, after.status, op) AS status,
    diff_value(before.network, after.network, op) AS network,
    diff_value(before.city, after.city, op) AS city,
    diff_value(before.timezone, after.timezone, op) AS timezone,
    diff_value(before.shop_id, after.shop_id, op) AS shop_id,

    if(
        isNotNull(diff_value(before.created_at, after.created_at, op) AS cr_at),
        toDateTime64(cr_at / 1e6, 3),
        NULL
    ) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_pickers
WHERE op IN ('c','u','d');

----------------------------------------------------------------------------------------------------


CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_products
TO clients.delivery_products_events
AS
SELECT
    coalesce(after.id, before.id) AS id,

    diff_value(before.title, after.title, op) AS title,
    diff_value(before.amount_type, after.amount_type, op) AS amount_type,
    diff_value(before.price_for_amount_item, after.price_for_amount_item, op) AS price_for_amount_item,
    diff_value(before.valid_hours, after.valid_hours, op) AS valid_hours,
    diff_value(before.total_amount, after.total_amount, op) AS total_amount,
    diff_value(before.discount_percent, after.discount_percent, op) AS discount_percent,

    if(
        isNotNull(diff_value(before.created_at, after.created_at, op) AS cr_at),
        toDateTime64(cr_at / 1e6, 3),
        NULL
    ) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_products
WHERE op IN ('c','u','d');

----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_clients
TO clients.delivery_clients_events
AS
SELECT
    coalesce(after.id, before.id) AS id,

    diff_value(before.full_name, after.full_name, op) AS full_name,
    diff_value(before.phone, after.phone, op) AS phone,
    diff_value(before.address, after.address, op) AS address,

    if(
        isNotNull(diff_value(before.created_at, after.created_at, op) AS cr_at),
        toDateTime64(cr_at / 1e6, 3),
        NULL
    ) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_clients
WHERE op IN ('c','u','d');

----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_orders
TO clients.delivery_orders_events
AS
SELECT
    coalesce(after.id, before.id) AS id,

    diff_value(before.city, after.city, op) AS city,
    diff_value(before.status, after.status, op) AS status,
    diff_value(before.shop_id, after.shop_id, op) AS shop_id,
    diff_value(before.client_id, after.client_id, op) AS client_id,
    diff_value(before.picker_id, after.picker_id, op) AS picker_id,
    diff_value(before.courier_id, after.courier_id, op) AS courier_id,
    diff_value(before.payment, after.payment, op) AS payment,
    if(
        isNotNull(diff_value(before.pay_date, after.pay_date, op) as pay_dt),
        toDateTime64(pay_dt / 1e6, 3),
        NULL
    ) AS pay_date,

    if(
        isNotNull(diff_value(before.collecting_start_date, after.collecting_start_date, op) as cs),
        toDateTime64(cs / 1e6, 3),
        NULL
    ) AS collecting_start_date,

    if(
        isNotNull(diff_value(before.collecting_end_date, after.collecting_end_date, op) AS ce),
        toDateTime64(ce / 1e6, 3),
        NULL
    ) AS collecting_end_date,

    if(
        isNotNull(diff_value(before.courier_assigned_date, after.courier_assigned_date, op) AS ca),
        toDateTime64(ca / 1e6, 3),
        NULL
    ) AS courier_assigned_date,

    if(
        isNotNull(diff_value(before.courier_take_date, after.courier_take_date, op) AS ct),
        toDateTime64(ct / 1e6, 3),
        NULL
    ) AS courier_take_date,

    if(
        isNotNull(diff_value(before.courier_delivered_date, after.courier_delivered_date, op) AS cd),
        toDateTime64(cd / 1e6, 3),
        NULL
    ) AS courier_delivered_date,

    if(
        isNotNull(diff_value(before.completed_date, after.completed_date, op) AS comp),
        toDateTime64(comp / 1e6, 3),
        NULL
    ) AS completed_date,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_orders
WHERE op IN ('c','u','d');


----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_order_products
TO clients.delivery_order_products_events
AS
SELECT
    coalesce(after.order_id, before.order_id) AS order_id,
    coalesce(after.product_id, before.product_id) AS product_id,

    diff_value(before.amount, after.amount, op) AS amount,
    diff_value(before.price, after.price, op) AS price,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_order_products
WHERE op IN ('c','u','d');

----------------------------------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS clients.mv_delivery_work_shifts
TO clients.delivery_work_shifts_events
AS
SELECT
    coalesce(after.id, before.id) AS id,

    if(
        isNotNull(diff_value(before.start_date, after.start_date, op) AS st),
        toDateTime64(st / 1e6, 3),
        NULL
    ) AS start_date,

    if(
        isNotNull(diff_value(before.end_date, after.end_date, op) AS en),
        toDateTime64(en / 1e6, 3),
        NULL
    ) AS end_date,

    if(
        isNotNull(diff_value(before.close_date, after.close_date, op) AS cl),
        toDateTime64(cl / 1e6, 3),
        NULL
    ) AS close_date,

    diff_value(before.courier_id, after.courier_id, op) AS courier_id,
    diff_value(before.status, after.status, op) AS status,
    diff_value(before.close_reason, after.close_reason, op) AS close_reason,

    if(
        isNotNull(diff_value(before.created_at, after.created_at, op) AS cr_at),
        toDateTime64(cr_at / 1e6, 3),
        NULL
    ) AS created_at,

    op = 'd' as is_deleted,
    ts_ms
FROM cdc.postgres_delivery_public_work_shifts
WHERE op IN ('c','u','d');
CREATE DATABASE IF NOT EXISTS clients
COMMENT 'Delivery kafka changes database';

-- Функция, которая определяет изменилось ли значение поля данных
CREATE FUNCTION IF NOT EXISTS diff_value AS (prev, new, op) ->
    if (
        op = 'd',
        NULL,
        if(
            isNull(new),
            NULL,
            if(
                isNull(prev),
                new,
                nullIf(new, prev)
            )
        )
    );

CREATE TABLE IF NOT EXISTS clients.delivery_polygons_events
(
    id UInt64 COMMENT 'ID полигона',
    name Nullable(String) COMMENT 'Название полигона доставки',
    city Nullable(String) COMMENT 'Город полигона',
    is_active Nullable(UInt8) COMMENT 'Флаг активности полигона (1 — активен)',
    created_at Nullable(DateTime64(3)) COMMENT 'Дата создания полигона в системе',

    is_deleted UInt8 COMMENT 'Была ли удалена хапись',
    ts_ms UInt64 COMMENT 'Время события CDC (epoch ms)'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_polygons.events',
    kafka_group_name = 'clients_delivery_polygons_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_shops_events
(
    id UInt64 COMMENT 'ID магазина',
    address Nullable(String) COMMENT 'Адрес магазина',
    delivery_enabled Nullable(UInt8) COMMENT 'Доступна ли доставка из магазина',
    start_work_time Nullable(String) COMMENT 'Время открытия магазина',
    end_work_time Nullable(String) COMMENT 'Время закрытия магазина',
    city Nullable(String) COMMENT 'Город магазина',
    polygon_id Nullable(UInt64) COMMENT 'Полигон доставки магазина',
    created_at Nullable(DateTime64(3)) COMMENT 'Дата создания магазина в системе',
    
    is_deleted UInt8 COMMENT 'Была ли удалена запись',
    ts_ms UInt64 COMMENT 'Время CDC события'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_shops.events',
    kafka_group_name = 'clients_delivery_shops_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_couriers_events
(
    id UInt64 COMMENT 'ID курьера',
    first_name Nullable(String) COMMENT 'Имя курьера',
    last_name Nullable(String) COMMENT 'Фамилия курьера',
    patronymic Nullable(String) COMMENT 'Отчество курьера',
    phone Nullable(String) COMMENT 'Телефон курьера',
    email Nullable(String) COMMENT 'Email курьера',
    inn Nullable(String) COMMENT 'ИНН курьера',
    city Nullable(String) COMMENT 'Город работы курьера',
    status Nullable(String) COMMENT 'Статус курьера (blocked/on_work/not_on_work)',
    company Nullable(String) COMMENT 'Компания курьера',
    self_employed Nullable(UInt8) COMMENT 'Самозанятый ли курьер',
    timezone Nullable(String) COMMENT 'Таймзона курьера',
    polygon_id Nullable(UInt64) COMMENT 'Полигон работы курьера',
    created_at Nullable(DateTime64(3)) COMMENT 'Дата создания курьера',

    is_deleted UInt8 COMMENT 'Была ли удалена запись',
    ts_ms UInt64 COMMENT 'Время CDC события'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_couriers.events',
    kafka_group_name = 'clients_delivery_couriers_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_pickers_events
(
    id UInt64 COMMENT 'ID сборщика заказов',
    first_name Nullable(String) COMMENT 'Имя сборщика',
    last_name Nullable(String) COMMENT 'Фамилия сборщика',
    patronymic Nullable(String) COMMENT 'Отчество',
    phone Nullable(String) COMMENT 'Телефон',
    email Nullable(String) COMMENT 'Email',
    status Nullable(String) COMMENT 'Статус (blocked/free/busy)',
    network Nullable(String) COMMENT 'Сеть работы сборщика',
    city Nullable(String) COMMENT 'Город',
    timezone Nullable(String) COMMENT 'Таймзона',
    shop_id Nullable(UInt64) COMMENT 'Магазин работы сборщика',
    created_at Nullable(DateTime64(3)) COMMENT 'Дата создания сборщика',

    is_deleted UInt8 COMMENT 'Была ли удалена запись',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_pickers.events',
    kafka_group_name = 'clients_delivery_pickers_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_products_events
(
    id UInt64 COMMENT 'ID товара',
    title Nullable(String) COMMENT 'Название товара',
    amount_type Nullable(String) COMMENT 'Тип измерения (шт/вес)',
    price_for_amount_item Nullable(Decimal(10,2)) COMMENT 'Цена за единицу',
    valid_hours Nullable(Int32) COMMENT 'Срок годности в часах',
    total_amount Nullable(Decimal(10,3)) COMMENT 'Общий доступный объём',
    discount_percent Nullable(Int32) COMMENT 'Скидка в процентах',
    created_at Nullable(DateTime64(3)) COMMENT 'Дата создания товара',

    is_deleted UInt8 COMMENT 'Была ли удалена запись',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_products.events',
    kafka_group_name = 'clients_delivery_products_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_clients_events
(
    id UInt64 COMMENT 'ID клиента',
    full_name Nullable(String) COMMENT 'ФИО клиента',
    phone Nullable(String) COMMENT 'Телефон клиента',
    address Nullable(String) COMMENT 'Адрес доставки',
    created_at Nullable(DateTime64(3)) COMMENT 'Дата создания клиента',

    is_deleted UInt8 COMMENT 'Была ли удалена запись',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_clients.events',
    kafka_group_name = 'clients_delivery_clients_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_orders_events
(
    id UInt64,
    city Nullable(String),
    status Nullable(String),
    shop_id Nullable(UInt64),
    client_id Nullable(UInt64),
    picker_id Nullable(UInt64),
    courier_id Nullable(UInt64),
    payment Nullable(UInt8),
    pay_date Nullable(DateTime64(3)),
    collecting_start_date Nullable(DateTime64(3)),
    collecting_end_date Nullable(DateTime64(3)),
    courier_assigned_date Nullable(DateTime64(3)),
    courier_take_date Nullable(DateTime64(3)),
    courier_delivered_date Nullable(DateTime64(3)),
    completed_date Nullable(DateTime64(3)),
    is_deleted UInt8,
    ts_ms UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_orders.events',
    kafka_group_name = 'clients_delivery_orders_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_order_products_events
(
    order_id UInt64 COMMENT 'ID заказа',
    product_id UInt64 COMMENT 'ID товара',
    amount Nullable(Decimal(10,3)) COMMENT 'Количество товара',
    price Nullable(Decimal(10,2)) COMMENT 'Цена позиции',

    is_deleted UInt8 COMMENT 'Была ли удалена запись',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_order_products.events',
    kafka_group_name = 'clients_delivery_order_products_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clients.delivery_work_shifts_events
(
    id UInt64 COMMENT 'ID смены',
    start_date Nullable(DateTime64(3)) COMMENT 'Начало смены',
    end_date Nullable(DateTime64(3)) COMMENT 'Конец смены',
    close_date Nullable(DateTime64(3)) COMMENT 'Дата закрытия смены',
    courier_id Nullable(UInt64) COMMENT 'Курьер смены',
    status Nullable(String) COMMENT 'Статус смены',
    close_reason Nullable(String) COMMENT 'Причина закрытия',
    created_at Nullable(DateTime64(3)) COMMENT 'Дата создания смены',

    is_deleted UInt8 COMMENT 'Была ли удалена запись',
    ts_ms UInt64 COMMENT 'Время CDC'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{{.Values.kafkaServer}}',
    kafka_topic_list = 'clients.delivery_work_shifts.events',
    kafka_group_name = 'clients_delivery_work_shifts_events',
    kafka_format = 'JSONEachRow';

----------------------------------------------------------------------------------------------------
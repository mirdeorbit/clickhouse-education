INSERT INTO polygons (name, city, is_active)
VALUES
('Center Zone', 'Moscow', true),
('North Zone', 'Moscow', true),
('South Zone', 'Saint Petersburg', true);

INSERT INTO shops (address, delivery_enabled, start_work_time, end_work_time, city, polygon_id)
VALUES
('Tverskaya 1', true, '08:00', '22:00', 'Moscow', 1),
('Arbat 10', true, '09:00', '21:00', 'Moscow', 1),
('Nevsky 25', false, '10:00', '20:00', 'Saint Petersburg', 3);


INSERT INTO couriers (first_name, last_name, patronymic, phone, email, inn, city, status, company, self_employed, timezone, polygon_id)
VALUES
('Ivan', 'Petrov', 'Ivanovich', '+79990000001', 'ivan@mail.ru', '1234567890', 'Moscow', 'on_work', 'Yandex', false, 'Europe/Moscow', 1),
('Petr', 'Sidorov', NULL, '+79990000002', NULL, NULL, 'Moscow', 'not_on_work', NULL, true, 'Europe/Moscow', 2),
('Anna', 'Smirnova', 'Olegovna', '+79990000003', 'anna@mail.ru', NULL, 'Saint Petersburg', 'blocked', 'DeliveryClub', false, 'Europe/Moscow', 3);


INSERT INTO pickers (first_name, last_name, patronymic, phone, email, status, network,city, timezone, shop_id)
VALUES
('Olga', 'Ivanova', NULL, '+79990000101', 'olga@mail.ru', 'free', 'asx', 'Moscow', 'Europe/Moscow', 1),
('Sergey', 'Kuznetsov', 'Petrovich', '+79990000102', NULL, 'busy', 'self_delivery', 'Moscow', 'Europe/Moscow', 2),
('Maria', 'Volkova', NULL, '+79990000103', 'maria@mail.ru', 'blocked', 'otus_logistics', 'Saint Petersburg', 'Europe/Moscow', 3);


INSERT INTO products ( title, amount_type, price_for_amount_item, date, valid_hours, total_amount, discount_percent)
VALUES
('Apples', 'weight', 120.50, CURRENT_DATE, 48, 100.000, 10),
('Milk 1L', 'items', 89.90, CURRENT_DATE, 72, 50.000, 0),
('Bananas', 'weight', 95.00, CURRENT_DATE, 36, 200.000, 5);


INSERT INTO clients (id, full_name, phone, address)
VALUES
(1, 'Alexey Morozov', '+79991111111', 'Lenina 10'),
(2, 'Dmitry Orlov', '+79992222222', 'Pushkina 5'),
(3, 'Elena Sokolova', '+79993333333', 'Sadovaya 7');


INSERT INTO orders (city, status, shop_id, client_id,picker_id, courier_id, payment, create_date, pay_date)
VALUES
('Moscow', 'created', 1, 1, 1, 1, false, NOW(), NULL),
('Moscow', 'delivering', 2, 2, 2, 2, true, NOW() - INTERVAL '2 hours', NOW() - INTERVAL '1 hour'),
('Saint Petersburg', 'completed', 3, 3, 3, 3, true, NOW() - INTERVAL '1 day', NOW() - INTERVAL '23 hours');


INSERT INTO order_products (order_id, product_id, amount, price)
VALUES
(1, 1, 1.500, 180.75),
(1, 2, 2.000, 179.80),
(2, 3, 3.000, 285.00),
(3, 1, 2.000, 241.00);


INSERT INTO work_shifts ( start_date, end_date, create_date, close_date, courier_id, status, close_reason)
VALUES
(NOW() - INTERVAL '8 hours', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '8 hours',  NOW() - INTERVAL '1 hour', 1, 'closed', 'successful_done'),
(NOW() - INTERVAL '2 hours', NULL, NOW() - INTERVAL '2 hours', NULL, 2, 'active', NULL),
(NOW() + INTERVAL '1 day', NULL, NOW(), NULL, 3, 'planned', NULL);
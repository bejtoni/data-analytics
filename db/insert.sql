-- Exchange rates
INSERT INTO ecommerce.exchange_rates (date, base_currency, target_currency, rate) VALUES
('2025-06-09 15:56:44', 'BRL', 'BAM', 99999.99999),
('2025-06-09 15:56:44', 'BRL', 'EUR', 99999.99999),
('2025-06-09 15:56:44', 'BRL', 'USD', 99999.99999);

-- Customers
INSERT INTO ecommerce.customers VALUES
('C2025A', 'CU2025A', 11111, 'Futurópolis', 'FT'),
('C2025B', 'CU2025B', 22222, 'Neo City', 'NC'),
('C2025C', 'CU2025C', 33333, 'Alphaville', 'AV'),
('C2025D', 'CU2025D', 44444, 'Cyberia', 'CB'),
('C2025E', 'CU2025E', 55555, 'Nova Lux', 'NL');

-- Sellers
INSERT INTO ecommerce.sellers VALUES
('S2025A', 66666, 'Robocity', 'RC'),
('S2025B', 77777, 'Silicon Port', 'SP'),
('S2025C', 88888, 'Quantum Town', 'QT'),
('S2025D', 99999, 'Nano Village', 'NV'),
('S2025E', 11112, 'Pixel Bay', 'PB');

-- Products
INSERT INTO ecommerce.products VALUES
('P2025A', 'ai_gadgets', 10, 100, 3, 250, 20, 10, 15),
('P2025B', 'robotics', 12, 120, 4, 300, 25, 15, 20),
('P2025C', 'wearables', 9, 90, 2, 180, 18, 9, 14),
('P2025D', 'iot_devices', 11, 110, 3, 220, 21, 11, 17),
('P2025E', 'smart_home', 13, 130, 5, 270, 23, 13, 16);

-- Orders
INSERT INTO ecommerce.orders VALUES
('O2025A', 'C2025A', 'delivered', '2025-01-10 10:00:00', '2025-01-10 10:30:00', '2025-01-10 11:00:00', '2025-01-10 12:00:00', '2025-01-15 00:00:00'),
('O2025B', 'C2025B', 'delivered', '2025-02-11 11:00:00', '2025-02-11 11:30:00', '2025-02-11 12:00:00', '2025-02-11 13:00:00', '2025-02-16 00:00:00'),
('O2025C', 'C2025C', 'delivered', '2025-03-12 12:00:00', '2025-03-12 12:30:00', '2025-03-12 13:00:00', '2025-03-12 14:00:00', '2025-03-17 00:00:00'),
('O2025D', 'C2025D', 'delivered', '2025-04-13 13:00:00', '2025-04-13 13:30:00', '2025-04-13 14:00:00', '2025-04-13 15:00:00', '2025-04-18 00:00:00'),
('O2025E', 'C2025E', 'delivered', '2025-05-14 14:00:00', '2025-05-14 14:30:00', '2025-05-14 15:00:00', '2025-05-14 16:00:00', '2025-05-19 00:00:00');

-- Order Items
INSERT INTO ecommerce.order_items VALUES
('O2025A', 1, 'P2025A', 'S2025A', '2025-01-10 17:00:00', 500.00, 25.00),
('O2025B', 1, 'P2025B', 'S2025B', '2025-02-11 17:00:00', 600.00, 30.00),
('O2025C', 1, 'P2025C', 'S2025C', '2025-03-12 17:00:00', 700.00, 35.00),
('O2025D', 1, 'P2025D', 'S2025D', '2025-04-13 17:00:00', 800.00, 40.00),
('O2025E', 1, 'P2025E', 'S2025E', '2025-05-14 17:00:00', 900.00, 45.00);

-- Payments
INSERT INTO ecommerce.order_payments VALUES
('O2025A', 1, 'credit_card', 1, 525.00),
('O2025B', 1, 'boleto', 2, 630.00),
('O2025C', 1, 'voucher', 1, 735.00),
('O2025D', 1, 'debit_card', 1, 840.00),
('O2025E', 1, 'credit_card', 3, 945.00);

-- Reviews
INSERT INTO ecommerce.order_reviews VALUES
('R2025A', 'O2025A', 5, 'Excellent', 'Fast and safe delivery!', '2025-01-11 10:00:00', '2025-01-11 10:05:00'),
('R2025B', 'O2025B', 4, 'Very Good', 'Smooth transaction.', '2025-02-12 11:00:00', '2025-02-12 11:05:00'),
('R2025C', 'O2025C', 3, 'Okay', 'Could be faster.', '2025-03-13 12:00:00', '2025-03-13 12:05:00'),
('R2025D', 'O2025D', 5, 'Perfect', 'Great service.', '2025-04-14 13:00:00', '2025-04-14 13:05:00'),
('R2025E', 'O2025E', 2, 'Disappointed', 'Arrived late.', '2025-05-15 14:00:00', '2025-05-15 14:05:00');

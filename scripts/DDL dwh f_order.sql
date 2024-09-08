/* Удаление таблицы, если она существует */
DROP TABLE IF EXISTS dwh.f_order;

-- Создание таблицы фактов "Заказы"
CREATE TABLE dwh.f_order (
    -- Идентификатор заказа (первичный ключ, автоинкремент)
    order_id BIGINT GENERATED ALWAYS AS IDENTITY,
    -- Идентификатор товара (int8)
    product_id int8 NOT NULL,
    -- Идентификатор мастера (int8)
    craftsman_id int8 NOT NULL,
    -- Идентификатор заказчика (int8)
    customer_id int8 NOT NULL,
    -- Дата создания заказа
    order_created_date DATE NOT NULL,
    -- Дата выполнения заказа
    order_completion_date DATE,
    -- Статус выполнения заказа (created, in progress, delivery, done)
    order_status VARCHAR CHECK (order_status IN ('created', 'in progress', 'delivery', 'done')) NOT NULL,
    -- Дата и время загрузки записи
    load_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,

    -- Внешний ключ для поля product_id
    CONSTRAINT orders_product_fk FOREIGN KEY (product_id) 
    REFERENCES dwh.d_product(product_id) 
    ON DELETE RESTRICT ON UPDATE RESTRICT,

    -- Внешний ключ для поля craftsman_id
    CONSTRAINT orders_craftsman_fk FOREIGN KEY (craftsman_id) 
    REFERENCES dwh.d_craftsman(craftsman_id) 
    ON DELETE RESTRICT ON UPDATE RESTRICT,

    -- Внешний ключ для поля customer_id
    CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id) 
    REFERENCES dwh.d_customer(customer_id) 
    ON DELETE RESTRICT ON UPDATE RESTRICT,

    -- Первичный ключ
    CONSTRAINT orders_pk PRIMARY KEY (order_id)
);

-- Добавление комментариев для таблицы и полей
COMMENT ON TABLE dwh.f_order IS 'Таблица фактов, содержащая данные о заказах, включая идентификаторы товаров, мастеров, заказчиков, даты и статусы заказов.';
COMMENT ON COLUMN dwh.f_order.order_id IS 'Идентификатор заказа';
COMMENT ON COLUMN dwh.f_order.product_id IS 'Идентификатор товара';
COMMENT ON COLUMN dwh.f_order.craftsman_id IS 'Идентификатор мастера';
COMMENT ON COLUMN dwh.f_order.customer_id IS 'Идентификатор заказчика';
COMMENT ON COLUMN dwh.f_order.order_created_date IS 'Дата создания заказа';
COMMENT ON COLUMN dwh.f_order.order_completion_date IS 'Дата выполнения заказа';
COMMENT ON COLUMN dwh.f_order.order_status IS 'Статус выполнения заказа (created, in progress, delivery, done)';
COMMENT ON COLUMN dwh.f_order.load_dttm IS 'Дата и время загрузки записи';
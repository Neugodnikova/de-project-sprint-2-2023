-- Создание измерения "Товары"
DROP TABLE IF EXISTS dwh.d_product;
CREATE TABLE dwh.d_product (
	product_id int8 GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
	product_name varchar NOT NULL,
	product_description varchar NOT NULL,
	product_type varchar NOT NULL,
	product_price int8 NOT NULL,
	load_dttm timestamp NOT NULL,
	CONSTRAINT products_pk PRIMARY KEY (product_id)
)

-- Создание измерения "Покупатели"
DROP TABLE IF EXISTS dwh.d_customer;
CREATE TABLE dwh.d_customer (
	customer_id int8 GENERATED ALWAYS AS IDENTITY( MINVALUE 0 NO MAXVALUE START 0 NO CYCLE) NOT NULL,
	customer_name varchar NULL,
	customer_address varchar NULL,
	customer_birthday date NULL,
	customer_email varchar NOT NULL,
	load_dttm timestamp NOT NULL,
	CONSTRAINT customers_pk PRIMARY KEY (customer_id)
);

-- Создание измерения "Мастера"
DROP TABLE IF EXISTS dwh.d_craftsman;
CREATE TABLE dwh.d_craftsman (
	craftsman_id int8 GENERATED ALWAYS AS IDENTITY( MINVALUE 0 NO MAXVALUE START 0 NO CYCLE) NOT NULL,
	craftsman_name varchar NOT NULL,
	craftsman_address varchar NOT NULL,
	craftsman_birthday date NOT NULL,
	craftsman_email varchar NOT NULL,
	load_dttm timestamp NOT NULL,
	CONSTRAINT craftsman_pk PRIMARY KEY (craftsman_id)
);
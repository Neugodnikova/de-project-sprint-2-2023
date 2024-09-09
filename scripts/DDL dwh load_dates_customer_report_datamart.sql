WITH
dwh_delta AS (
    SELECT     
        dc.customer_id AS customer_id,
        dc.customer_name AS customer_name,
        dc.customer_address AS customer_address,
        dc.customer_birthday AS customer_birthday,
        dc.customer_email AS customer_email,
        fo.order_id AS order_id,
        dp.product_id AS product_id,
        dp.product_price AS product_price,
        dp.product_type AS product_type,
        fo.craftsman_id AS craftsman_id,
        fo.order_completion_date - fo.order_created_date AS diff_order_date,
        fo.order_status AS order_status,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        dc.load_dttm AS customer_load_dttm,
        dp.load_dttm AS products_load_dttm,
        dcm.craftsman_name AS craftsman_name
    FROM dwh.f_order fo 
    INNER JOIN dwh.d_customer dc ON fo.customer_id = dc.customer_id 
    INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id 
    INNER JOIN dwh.d_craftsman dcm ON fo.craftsman_id = dcm.craftsman_id
    LEFT JOIN dwh.customer_report_datamart crd ON dc.customer_id = crd.customer_id
    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),
dwh_update_delta AS (
    SELECT     
        dd.customer_id AS customer_id
    FROM dwh_delta dd 
    WHERE dd.customer_id IS NOT NULL        
),
dwh_delta_insert_result AS (
    SELECT  
        ROW_NUMBER() OVER() AS record_id, -- Добавлено: идентификатор записи
        T4.customer_id AS customer_id,
        T4.customer_name AS customer_name,
        T4.customer_address AS customer_address,
        T4.customer_birthday AS customer_birthday,
        T4.customer_email AS customer_email,
        T4.customer_spent AS customer_spent,
        T4.platform_earnings AS platform_earnings,
        T4.order_count AS order_count,
        T4.avg_order_price AS avg_order_price,
        T4.median_order_completion_time AS median_order_completion_time,
        T4.product_type AS top_product_category,
        T4.top_craftsman_id AS top_craftsman_id,
        T4.count_order_created AS count_order_created,
        T4.count_order_in_progress AS count_order_in_progress,
        T4.count_order_delivery AS count_order_delivery,
        T4.count_order_done AS count_order_done,
        T4.count_order_not_done AS count_order_not_done,
        T4.report_period AS report_period
    FROM (
        SELECT
            *,
            RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product 
        FROM ( 
            SELECT
                T1.customer_id AS customer_id,
                T1.customer_name AS customer_name,
                T1.customer_address AS customer_address,
                T1.customer_birthday AS customer_birthday,
                T1.customer_email AS customer_email,
                SUM(T1.product_price) AS customer_spent,
                SUM(T1.product_price) * 0.1 AS platform_earnings,
                COUNT(order_id) AS order_count,
                AVG(T1.product_price) AS avg_order_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_order_completion_time,
                T1.product_type AS top_product_category,
                T1.craftsman_id AS top_craftsman_id,
                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                T1.report_period AS report_period
            FROM dwh_delta AS T1
            GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period, T1.craftsman_id
        ) AS T2
        INNER JOIN (
            SELECT
                dd.customer_id AS customer_id_for_product_type,
                dd.product_type,
                COUNT(dd.product_id) AS count_product
            FROM dwh_delta AS dd
            GROUP BY dd.customer_id, dd.product_type
            ORDER BY count_product DESC
        ) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
    ) AS T4
    WHERE T4.rank_count_product = 1 
    ORDER BY report_period
),
dwh_delta_update_result AS (
    SELECT 
        ROW_NUMBER() OVER() AS record_id, -- Добавлено: идентификатор записи
        T4.customer_id AS customer_id,
        T4.customer_name AS customer_name,
        T4.customer_address AS customer_address,
        T4.customer_birthday AS customer_birthday,
        T4.customer_email AS customer_email,
        T4.customer_spent AS customer_spent,
        T4.platform_earnings AS platform_earnings,
        T4.order_count AS order_count,
        T4.avg_order_price AS avg_order_price,
        T4.median_order_completion_time AS median_order_completion_time,
        T4.product_type AS top_product_category,
        T4.top_craftsman_id AS top_craftsman_id,
        T4.count_order_created AS count_order_created,
        T4.count_order_in_progress AS count_order_in_progress,
        T4.count_order_delivery AS count_order_delivery,
        T4.count_order_done AS count_order_done,
        T4.count_order_not_done AS count_order_not_done,
        T4.report_period AS report_period 
    FROM (
        SELECT
            *,
            RANK() OVER(PARTITION BY T2.customer_id ORDER BY count_product DESC) AS rank_count_product 
        FROM (
            SELECT
                T1.customer_id AS customer_id,
                T1.customer_name AS customer_name,
                T1.customer_address AS customer_address,
                T1.customer_birthday AS customer_birthday,
                T1.customer_email AS customer_email,
                SUM(T1.product_price) AS customer_spent,
                SUM(T1.product_price) * 0.1 AS platform_earnings,
                COUNT(order_id) AS order_count,
                AVG(T1.product_price) AS avg_order_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_order_completion_time,
                T1.product_type AS top_product_category,
                T1.craftsman_id AS top_craftsman_id,
                SUM(CASE WHEN T1.order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                SUM(CASE WHEN T1.order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                SUM(CASE WHEN T1.order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                SUM(CASE WHEN T1.order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
                SUM(CASE WHEN T1.order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done,
                T1.report_period AS report_period
            FROM dwh_delta AS T1
            GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period, T1.craftsman_id
        ) AS T2
        INNER JOIN (
            SELECT
                dd.customer_id AS customer_id_for_product_type,
                dd.product_type,
                COUNT(dd.product_id) AS count_product
            FROM dwh_delta AS dd
            GROUP BY dd.customer_id, dd.product_type
            ORDER BY count_product DESC
        ) AS T3 ON T2.customer_id = T3.customer_id_for_product_type
    ) AS T4
    WHERE T4.rank_count_product = 1 
    ORDER BY report_period
)

-- Вставка новых данных в таблицу витрины
INSERT INTO dwh.customer_report_datamart (
    record_id, customer_id, customer_name, customer_address, customer_birthday, customer_email, 
    customer_spent, platform_earnings, order_count, avg_order_price, 
    median_order_completion_time, top_product_category, top_craftsman_id, 
    count_order_created, count_order_in_progress, count_order_delivery, 
    count_order_done, count_order_not_done, report_period
)
SELECT * FROM dwh_delta_insert_result
ON CONFLICT (customer_id, report_period) 
DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    customer_address = EXCLUDED.customer_address,
    customer_birthday = EXCLUDED.customer_birthday,
    customer_email = EXCLUDED.customer_email,
    customer_spent = EXCLUDED.customer_spent,
    platform_earnings = EXCLUDED.platform_earnings,
    order_count = EXCLUDED.order_count,
    avg_order_price = EXCLUDED.avg_order_price,
    median_order_completion_time = EXCLUDED.median_order_completion_time,
    top_product_category = EXCLUDED.top_product_category,
    top_craftsman_id = EXCLUDED.top_craftsman_id,
    count_order_created = EXCLUDED.count_order_created,
    count_order_in_progress = EXCLUDED.count_order_in_progress,
    count_order_delivery = EXCLUDED.count_order_delivery,
    count_order_done = EXCLUDED.count_order_done,
    count_order_not_done = EXCLUDED.count_order_not_done;

-- Запись даты последней загрузки данных в таблицу отслеживания
INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
SELECT CURRENT_DATE;

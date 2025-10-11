INSERT INTO dim_dates (date_id, full_date, year, quarter, month, day, week, is_weekend)
SELECT
    to_char(d, 'YYYYMMDD')::INTEGER AS date_id,
    d AS full_date,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(MONTH FROM d) AS month,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(WEEK FROM d) AS week,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2020-01-01'::TIMESTAMP, '2030-12-31'::TIMESTAMP, INTERVAL '1 day') d;

WITH ui_ingestiones AS (
    SELECT
        ingestion_year,
        ingestion_month,
        ingestion_day
    FROM {zonatabla}
    GROUP BY 1,2,3
    ORDER BY 1 DESC, 2 DESC, 3 DESC
    LIMIT 1
)
SELECT
    t1.tabla,
    t1.campo1,
    t1.campo2,
    t1.campo3
FROM {zonatabla} t1
INNER JOIN ui_ingestiones t2
    ON t2.ingestion_year = t1.ingestion_year
    AND t2.ingestion_month = t1.ingestion_month
    AND t2.ingestion_day = t1.ingestion_day


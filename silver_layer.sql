-- Criação do Silver Layer (Camada com dados tratados)

CREATE OR REPLACE TABLE silver_finance_data AS
SELECT 
    symbol,
    AVG(price) AS avg_price,
    SUM(volume) AS total_volume,
    date_trunc('day', date) AS date
FROM 
    finance_data
GROUP BY 
    symbol, date_trunc('day', date)
HAVING 
    COUNT(*) > 1;

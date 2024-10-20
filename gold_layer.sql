-- Criação do Gold Layer (Agregações e análises das top ações)

CREATE OR REPLACE TABLE gold_finance_analytics AS
WITH ranked_stocks AS (
    SELECT 
        symbol, 
        total_volume, 
        avg_price,
        date,  -- Adicionando a coluna date
        RANK() OVER (ORDER BY total_volume DESC) AS rank_volume
    FROM silver_finance_data
),

stock_performance AS (
    SELECT
        symbol,
        total_volume,
        avg_price,
        date,  -- Mantendo a coluna date
        LAG(avg_price, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_avg_price,
        -- Cálculo da variação percentual
        (avg_price - LAG(avg_price, 1) OVER (PARTITION BY symbol ORDER BY date)) AS price_variation,
        -- Cálculo da variação percentual em relação ao dia anterior
        CASE 
            WHEN LAG(avg_price, 1) OVER (PARTITION BY symbol ORDER BY date) IS NULL THEN NULL
            ELSE (avg_price - LAG(avg_price, 1) OVER (PARTITION BY symbol ORDER BY date)) / LAG(avg_price, 1) OVER (PARTITION BY symbol ORDER BY date) * 100
        END AS pct_price_variation,
        -- Identificação de tendência
        CASE 
            WHEN avg_price > LAG(avg_price, 1) OVER (PARTITION BY symbol ORDER BY date) THEN 'Bullish'
            WHEN avg_price < LAG(avg_price, 1) OVER (PARTITION BY symbol ORDER BY date) THEN 'Bearish'
            ELSE 'Stable'
        END AS price_trend
    FROM ranked_stocks
    WHERE rank_volume <= 3
),

final_analytics AS (
    SELECT
        symbol,
        total_volume,
        avg_price,
        prev_avg_price,
        price_variation,
        pct_price_variation,
        price_trend,
        -- Volume médio nos últimos 7 dias
        AVG(total_volume) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_volume_7_days,
        -- Desvio padrão do preço nos últimos 7 dias
        STDDEV(avg_price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS stddev_price_7_days  
    FROM stock_performance
)

SELECT 
    symbol,
    total_volume,
    avg_price,
    prev_avg_price,
    price_variation,
    pct_price_variation,
    price_trend,
    avg_volume_7_days,
    stddev_price_7_days
FROM final_analytics
ORDER BY total_volume DESC;

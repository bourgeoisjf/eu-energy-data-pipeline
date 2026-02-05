CREATE OR REPLACE VIEW view_hourly_solar_profitability AS
WITH daily_prices AS (
    -- Calculamos a média diária primeiro para garantir que temos um preço para o dia
    SELECT 
        country, 
        DATE(start_time) as price_day, 
        AVG(price_eur) as day_avg_price
    FROM energy_prices
    GROUP BY 1, 2
),
hourly_gen AS (
    -- Pegamos a geração horária
    SELECT 
        country,
        DATE(start_time) as gen_day,
        EXTRACT(HOUR FROM start_time) as hour_of_day,
        SUM(quantity_mw) as solar_mw
    FROM energy_generation
    WHERE generation_type ILIKE '%Solar%'
    GROUP BY 1, 2, 3
)
SELECT 
    g.country,
    g.gen_day as day,
    g.hour_of_day,
    ROUND(p.day_avg_price, 2) as ref_price_eur,
    ROUND(g.solar_mw, 2) as solar_mw,
    ROUND(g.solar_mw * p.day_avg_price, 2) as estimated_revenue_eur,
    RANK() OVER (PARTITION BY g.country, g.gen_day ORDER BY g.solar_mw DESC) as solar_peak_rank
FROM hourly_gen g
INNER JOIN daily_prices p ON g.country = p.country AND g.gen_day = p.price_day;
CREATE OR REPLACE VIEW view_daily_solar_revenue AS
WITH daily_stats AS (
    SELECT 
        g.country,
        DATE(g.start_time) as day,
        AVG(p.price_eur) as avg_price_eur,
        SUM(g.quantity_mw) FILTER (WHERE g.generation_type ILIKE '%Solar%') as total_solar_mw,
        SUM(g.quantity_mw) as total_gen_mw
    FROM energy_generation g
    INNER JOIN energy_prices p ON g.country = p.country 
         AND DATE(g.start_time) = DATE(p.start_time)
    GROUP BY 1, 2
)
SELECT 
    country,
    day,
    ROUND(avg_price_eur, 2) as avg_price_eur,
    ROUND(total_solar_mw, 2) as total_solar_mw,
    ROUND((total_solar_mw * avg_price_eur), 2) as estimated_solar_revenue_eur,
    ROUND((total_solar_mw / NULLIF(total_gen_mw, 0)) * 100, 3) as solar_percentage
FROM daily_stats;
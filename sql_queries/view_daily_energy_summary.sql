CREATE OR REPLACE VIEW daily_energy_summary AS
SELECT 
    g.country,
    DATE(g.start_time) as day,
    ROUND(AVG(p.price_eur), 2) as avg_price_eur,
    ROUND(SUM(g.quantity_mw) FILTER (WHERE g.generation_type ILIKE '%Solar%'), 2) as total_solar_mw,
    ROUND(SUM(g.quantity_mw), 2) as total_gen_mw,
    ROUND(
        (SUM(g.quantity_mw) FILTER (WHERE g.generation_type ILIKE '%Solar%') / 
        NULLIF(SUM(g.quantity_mw), 0)) * 100, 3
    ) as solar_percentage
FROM energy_generation g
INNER JOIN energy_prices p ON g.country = p.country 
     AND DATE(g.start_time) = DATE(p.start_time)
GROUP BY 1, 2;
CREATE OR REPLACE VIEW ranking_best_worst_gen_type AS
WITH Raw_Generation_Stats AS (
    SELECT
        g.country_name AS country,
        g.generation_type,
        -- Aggregate energy production and pricing
        ROUND(SUM(g.quantity_mw), 2) AS total_mw_h,
        ROUND(AVG(p.price_eur), 3) AS average_price,
        
        -- Ranking logic: 1 is the highest production
        RANK() OVER (
            PARTITION BY g.country_name 
            ORDER BY SUM(g.quantity_mw) DESC
        ) AS rank_best,
        
        -- Ranking logic: 1 is the lowest production
        RANK() OVER (
            PARTITION BY g.country_name 
            ORDER BY SUM(g.quantity_mw) ASC
        ) AS rank_worst,
        
        -- Calculate the percentage share of each source within the country
        ROUND(
            (SUM(g.quantity_mw) / SUM(SUM(g.quantity_mw)) OVER (PARTITION BY g.country_name)) * 100, 
            2
        ) AS market_share_pct
    FROM energy_generation g
    LEFT JOIN energy_prices p 
        ON p.country = g.country
    GROUP BY 
        g.country_name, 
        g.generation_type
)

SELECT 
    country,
    -- Best Generator Block
    MAX(CASE WHEN rank_best = 1 THEN generation_type END) AS top_generation_source,
    MAX(CASE WHEN rank_best = 1 THEN total_mw_h END) AS top_generation_volume,
    MAX(CASE WHEN rank_best = 1 THEN market_share_pct END) AS leader_market_share_pct,
    
    -- Worst Generator Block
    MAX(CASE WHEN rank_worst = 1 THEN generation_type END) AS worst_generation_source,
    MAX(CASE WHEN rank_worst = 1 THEN total_mw_h END) AS worst_generation_volume,
    
    -- National pricing context
    MAX(average_price) AS country_avg_price_eur
FROM Raw_Generation_Stats
WHERE 
    rank_best = 1 OR rank_worst = 1
GROUP BY 
    country
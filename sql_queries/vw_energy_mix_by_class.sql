CREATE OR REPLACE VIEW vw_energy_mix_by_class AS
WITH Categorized_Data AS (
    SELECT 
        country_name,
        quantity_mw,
        CASE 
            WHEN generation_type ILIKE '%Fossil%' 
                 OR generation_type ILIKE '%Coal%' 
                 OR generation_type ILIKE '%Nuclear%' 
                 THEN 'Non-Renewable'
            ELSE 'Renewable'
        END AS energy_class
    FROM energy_generation
)
SELECT 
    country_name,
    energy_class,
    TO_CHAR(SUM(quantity_mw), '"MW/H "999G999G999G999D00') AS total_mw,
    ROUND(
        (SUM(quantity_mw) / SUM(SUM(quantity_mw)) OVER(PARTITION BY country_name)) * 100, 
        2
    ):: TEXT || '%' AS percentage_share
FROM Categorized_Data
GROUP BY 
    country_name, 
    energy_class
ORDER BY 
    country_name, 
    SUM(quantity_mw) DESC;
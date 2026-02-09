-- 1. Criar a função que faz o cálculo
CREATE OR REPLACE FUNCTION compute_entsoe_time(ts timestamptz, pos integer) 
RETURNS timestamptz AS $$
BEGIN
    RETURN ts + (pos - 1) * INTERVAL '15 minutes';
END;
$$ LANGUAGE plpgsql IMMUTABLE; -- O segredo está aqui!

-- 2. Adicionar a coluna usando a sua função
ALTER TABLE energy_prices 
ADD COLUMN actual_timestamp_utc timestamp 
GENERATED ALWAYS AS (
    (start_time AT TIME ZONE 'UTC' + (position - 1) * INTERVAL '15 minutes')
) STORED;
SELECT
    t.table_name,
    COALESCE(ARRAY_AGG(DISTINCT ccu.table_name) FILTER (WHERE ccu.table_name != tc.table_name), ARRAY[]::VARCHAR[]) AS foreign_tables
FROM information_schema.tables t

    LEFT JOIN information_schema.table_constraints tc ON tc.table_name = t.table_name
    LEFT JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.table_schema='public' AND ccu.table_name IS NOT NULL
GROUP BY t.table_name
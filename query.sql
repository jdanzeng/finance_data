SELECT name,hour(cast(ts AS timestamp)) AS time, max(high) AS max_high
FROM "23"
GROUP BY  name,hour(cast(ts AS timestamp));

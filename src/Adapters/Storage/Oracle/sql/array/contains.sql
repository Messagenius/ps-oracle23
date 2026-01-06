CREATE OR REPLACE FUNCTION array_contains(
  p_array   CLOB,
  p_values  CLOB
)
  RETURN NUMBER
  DETERMINISTIC
IS
  l_count NUMBER;
BEGIN
SELECT COUNT(*)
INTO l_count
FROM JSON_TABLE(p_array, '$[*]'
    COLUMNS (value VARCHAR2(4000) PATH '$')) arr
WHERE arr.value IN (
    SELECT val.value
    FROM JSON_TABLE(p_values, '$[*]'
        COLUMNS (value VARCHAR2(4000) PATH '$')) val
)
  AND ROWNUM = 1;

RETURN CASE WHEN l_count >= 1 THEN 1 ELSE 0 END;
END array_contains;
/

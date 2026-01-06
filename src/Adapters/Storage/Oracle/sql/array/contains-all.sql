CREATE OR REPLACE FUNCTION array_contains_all(
  p_array   CLOB,
  p_values  CLOB
)
  RETURN NUMBER
  DETERMINISTIC
IS
  l_values_count NUMBER;
  l_match_count  NUMBER;
BEGIN
  -- Get the count of elements in the values array
SELECT COUNT(*)
INTO l_values_count
FROM JSON_TABLE(p_values, '$[*]'
    COLUMNS (value VARCHAR2(4000) PATH '$'));

-- If values array is empty, return false (0)
IF l_values_count = 0 THEN
    RETURN 0;
END IF;

  -- Count how many values exist in the array
SELECT COUNT(DISTINCT val.value)
INTO l_match_count
FROM JSON_TABLE(p_values, '$[*]'
    COLUMNS (value VARCHAR2(4000) PATH '$')) val
WHERE val.value IN (
    SELECT arr.value
    FROM JSON_TABLE(p_array, '$[*]'
        COLUMNS (value VARCHAR2(4000) PATH '$')) arr
);

-- Return 1 (true) if all values found, 0 (false) otherwise
RETURN CASE WHEN l_match_count = l_values_count THEN 1 ELSE 0 END;
END array_contains_all;
/

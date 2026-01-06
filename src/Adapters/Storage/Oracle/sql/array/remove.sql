CREATE OR REPLACE FUNCTION array_remove(
  p_array   CLOB,
  p_values  CLOB
)
  RETURN CLOB
  DETERMINISTIC
IS
  l_result CLOB;
BEGIN
SELECT JSON_ARRAYAGG(arr.value ORDER BY arr.value RETURNING CLOB)
INTO l_result
FROM JSON_TABLE(p_array, '$[*]'
    COLUMNS (value VARCHAR2(4000) PATH '$')) arr
WHERE arr.value NOT IN (
    SELECT val.value
    FROM JSON_TABLE(p_values, '$[*]'
        COLUMNS (value VARCHAR2(4000) PATH '$')) val
);

-- Handle case where all elements are removed (return empty array)
IF l_result IS NULL THEN
    l_result := '[]';
END IF;

RETURN l_result;
END array_remove;
/
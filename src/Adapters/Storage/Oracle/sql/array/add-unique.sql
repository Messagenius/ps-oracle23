CREATE OR REPLACE FUNCTION array_add_unique(
  p_array   CLOB,
  p_values  CLOB
)
  RETURN CLOB
  DETERMINISTIC
IS
  l_result CLOB;
BEGIN
SELECT JSON_ARRAYAGG(DISTINCT value ORDER BY value RETURNING CLOB)
INTO l_result
FROM (
         SELECT jt.value
         FROM JSON_TABLE(p_array, '$[*]'
             COLUMNS (value VARCHAR2(4000) PATH '$')) jt
         UNION
         SELECT jt.value
         FROM JSON_TABLE(p_values, '$[*]'
             COLUMNS (value VARCHAR2(4000) PATH '$')) jt
     );

RETURN l_result;
END array_add_unique;
/

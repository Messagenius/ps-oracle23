CREATE OR REPLACE FUNCTION array_add(
  p_array   CLOB,
  p_values  CLOB
)
RETURN CLOB
IS
  l_result CLOB;
BEGIN
SELECT JSON_ARRAYAGG(
               JSON_VALUE(value, '$') FORMAT JSON
           ORDER BY value
       )
INTO l_result
FROM (
         SELECT DISTINCT value
         FROM (
                  -- Элементы из первого массива
                  SELECT COLUMN_VALUE as value
                  FROM JSON_TABLE(p_array, '$[*]'
                      COLUMNS (COLUMN_VALUE VARCHAR2(4000) PATH '$'))

                  UNION

                  -- Элементы из второго массива
                  SELECT COLUMN_VALUE as value
                  FROM JSON_TABLE(p_values, '$[*]'
                      COLUMNS (COLUMN_VALUE VARCHAR2(4000) PATH '$'))
              )
     );

RETURN l_result;
END array_add;
/
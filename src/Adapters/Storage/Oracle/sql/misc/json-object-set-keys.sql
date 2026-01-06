CREATE OR REPLACE FUNCTION json_object_set_key(
  p_json         CLOB,
  p_key_to_set   VARCHAR2,
  p_value_to_set CLOB
)
  RETURN CLOB
  DETERMINISTIC
IS
  l_json_obj JSON_OBJECT_T;
BEGIN
  -- Parse the JSON object
  l_json_obj := JSON_OBJECT_T(p_json);
  
  -- Set/update the key with the new value
  -- If p_value_to_set is already JSON, parse it; otherwise treat as string
BEGIN
    l_json_obj.put(p_key_to_set, JSON_ELEMENT_T.parse(p_value_to_set));
EXCEPTION
    WHEN OTHERS THEN
      -- If not valid JSON, treat as string
      l_json_obj.put(p_key_to_set, p_value_to_set);
END;
  
  -- Return the modified JSON
RETURN l_json_obj.to_clob();
END json_object_set_key;
/
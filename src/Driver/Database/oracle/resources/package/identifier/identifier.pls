create or replace package identifier
    AUTHID CURRENT_USER
as

  type vc_arr is table of varchar2(4000 char) index by binary_integer;

  function long_identifier_prefix
  return varchar2;

  function identifier_max_length
  return number;

  function empty_replacer_char
  return varchar2;

  function get_for(p_long_identifier varchar2)
  return varchar2;

  function check_db_prefix(p_db_prefix varchar2)
  return varchar2;

end identifier;


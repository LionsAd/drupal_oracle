create or replace package identifier
    AUTHID CURRENT_USER
as

  type serial_info is record
  (
      sequence_name    varchar2(30),
      field_name       varchar2(30),
      trigger_name     varchar2(30),
      sequence_restart number
  );

  type serial_info_tab is table of serial_info;

  type vc_list is table of varchar2(4000 char);

  type vc_arr is table of varchar2(4000 char) index by binary_integer;

  function long_identifier_prefix
  return varchar2;

  function identifier_max_length
  return number;

  function empty_replacer_char
  return varchar2;

  function get_for(p_long_identifier varchar2)
  return varchar2;

  function sequence_for_table(p_table varchar2, p_schema varchar2)
  return varchar2;

  function get_serial(p_table varchar2, p_schema varchar2)
  return serial_info_tab
  pipelined;

  function check_db_prefix(p_db_prefix varchar2)
  return varchar2;

  function longin(p_val vargst)
  return vc_list
  pipelined;

end identifier;


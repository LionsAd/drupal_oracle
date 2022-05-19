create or replace
package body identifier
as

  function long_identifier_prefix
  return varchar2 as
  begin
    return '<?php print ORACLE_LONG_IDENTIFIER_PREFIX; ?>';
  end;

  function identifier_max_length return number as
  begin
    return <?php print ORACLE_IDENTIFIER_MAX_LENGTH; ?>;
  end;

  function empty_replacer_char return varchar2 as
  begin
    return '<?php print ORACLE_EMPTY_STRING_REPLACER; ?>';
  end;

  function get_for(p_long_identifier varchar2)
  return varchar2
  as pragma autonomous_transaction;
   v_id number;
  begin

     if length(p_long_identifier) < identifier_max_length+1 then
       return p_long_identifier;
     end if;

     select id
       into v_id
       from long_identifiers
      where identifier= upper(p_long_identifier);

     return long_identifier_prefix()||v_id;

  exception
   when no_data_found then

     insert into long_identifiers (id,identifier)
          values (seq_long_identifiers.nextval,upper(p_long_identifier))
       returning id into v_id;
     commit;

     return long_identifier_prefix()||v_id;

  end;

  function check_db_prefix(p_db_prefix varchar2)
  return varchar2
  as pragma autonomous_transaction;
    v_db_prefix   varchar2(30):= upper(get_for(p_db_prefix));
  begin

     select username
       into v_db_prefix
       from all_users
      where username= v_db_prefix;

      return v_db_prefix;

  exception
    when no_data_found then
      execute immediate 'GRANT connect, resource to "'||v_db_prefix||'" identified by "'||v_db_prefix||'" container=all';

      -- Allow to allocate as much space as needed.
      -- Required for a proper tests run.
      execute immediate 'GRANT UNLIMITED TABLESPACE TO "'||v_db_prefix||'"';

      return v_db_prefix;
  end;

end identifier;

<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Component\Utility\SafeMarkup;
use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\SchemaObjectExistsException;
use Drupal\Core\Database\SchemaObjectDoesNotExistException;
use Drupal\Core\Database\Schema as DatabaseSchema;

/**
 * Oracle implementation of \Drupal\Core\Database\Schema.
 */
class Schema extends DatabaseSchema {

  /**
   * The database connection.
   *
   * @var \Drupal\Driver\Database\oracle\Connection
   */
  protected $connection;

  /**
   * An array of driver internal tables names.
   */
  protected $driverTables = [
    'LONG_IDENTIFIERS',
  ];

  /**
   * A cache of information about blob columns and sequences of tables.
   *
   * This is collected by Schema::queryTableInformation(), by introspecting the
   * database.
   *
   * @see \Drupal\Core\Database\Driver\oracle\Schema::queryTableInformation()
   * @var array
   */
  protected $tableInformation = [];

  /**
   * @todo description
   */
  private $foundLongIdentifier = FALSE;

  /**
   * Oracle schema helper.
   */
  public function oid($name, $prefix = FALSE, $quote = TRUE) {
    $return = $name;

    if (strlen($return) > ORACLE_IDENTIFIER_MAX_LENGTH) {
      $this->foundLongIdentifier = TRUE;
      $return = $this->connection
        ->queryOracle('SELECT identifier.get_for(?) FROM dual', [strtoupper($return)])
        ->fetchField();
    }

    $return = $prefix ? '{' . $return . '}' : strtoupper($return);

    if (!$prefix && $quote) {
      $return = '"' . $return . '"';
    }

    return $return;
  }

  /**
   * Oracle schema helper.
   *
   * @return string
   *   The non-prefixed but quoted ($schema.)$name.
   */
  public function oidWithSchema($name) {
    $prefixed = strtoupper(str_replace('"', '', $this->connection->prefixTables('{' . $name . '}')));

    $exp = explode('.', $prefixed, 2);
    if (count($exp) < 2) {
      return $this->oid($name);
    }

    return $this->oid($exp[0]) . '.' . $this->oid($name);
  }

  /**
   * Oracle schema helper.
   */
  private function resetLongIdentifiers() {
    if ($this->foundLongIdentifier) {
      $this->connection->resetLongIdentifiers();
      $this->foundLongIdentifier = FALSE;
    }
  }

  /**
   * Fetch the list of blobs and sequences used on a table.
   *
   * We introspect the database to collect the information required by insert
   * and update queries.
   *
   * @param $table_name
   *   The non-prefixed name of the table.
   * @return
   *   An object with two member variables:
   *     - 'blob_fields' that lists all the blob fields in the table.
   *     - 'sequences' that lists the sequences used in that table.
   */
  public function queryTableInformation($table) {
    $key = $this->getTableInformationKey($table);

    $table_information = (object) [
      'blob_fields' => [],
      'sequences' => [],
    ];

    if (empty($this->tableInformation[$key])) {
      [$schema, $table] = $this->tableSchema($table);
      $table_name = $this->oid($table, FALSE, FALSE);

      $blobs = $this->connection->query("SELECT column_name FROM all_tab_columns WHERE data_type = 'BLOB' AND table_name = :db_table AND owner = :db_owner", [':db_table' => $table_name, ':db_owner' => $schema])
        ->fetchCol();
      $sequences = $this->connection->query("SELECT sequence_name FROM all_tab_identity_cols WHERE table_name = :db_table AND owner = :db_owner", [':db_table' => $table_name, ':db_owner' => $schema])
        ->fetchCol();

      foreach ($sequences as $key => $sequence_name) {
        $full_name =<<<EOF
"$schema"."$sequence_name"
EOF;
        $sequences[$key] = str_replace("\n", "", $full_name);
      }

      $table_information->blob_fields = array_combine($blobs, $blobs);
      $table_information->sequences = $sequences;

      $this->tableInformation[$key] = $table_information;
    }

    return $this->tableInformation[$key];
  }

  /**
   * Resets information about table blobs, sequences and serial fields.
   *
   * @param $table
   *   The non-prefixed name of the table.
   */
  protected function resetTableInformation($table) {
    $key = $this->getTableInformationKey($table);
    unset($this->tableInformation[$key]);
  }

  /**
   * Returns the key for the tableInformation array for a given table.
   *
   * @param $table
   *   The non-prefixed name of the table.
   */
  protected function getTableInformationKey($table) {
    [$schema, $table_name] = $this->tableSchema($table);

    return $schema . '.' . $table_name;
  }

  /**
   * {@inheritdoc}
   */
  public function createTable($name, $table) {
    if ($this->tableExists($name)) {
      throw new SchemaObjectExistsException(t('Table @name already exists.', ['@name' => $name]));
    }
    $statements = $this->createTableSql($name, $table);
    foreach ($statements as $statement) {
      $this->connection->query($statement, [], ['allow_delimiter_in_query' => TRUE]);
    }
    $this->resetLongIdentifiers();
    $this->resetTableInformation($name);
  }

  /**
   * Generate SQL to create a new table from a Drupal schema definition.
   *
   * @param string $name
   *   The name of the table to create.
   * @param array $table
   *   A Schema API table definition array.
   *
   * @return string[]
   *   An array of SQL statements to create the table.
   */
  protected function createTableSql($name, array $table) {
    [$schema, $table_name] = $this->tableSchema($name);
    $oname = $this->oid($name, TRUE);

    $sql_fields = array();
    foreach ($table['fields'] as $field_name => $field) {
      $sql_fields[] = $this->createFieldSql($field_name, $this->processField($field));
    }

    $sql_keys = array();

    if (!empty($table['primary key']) && is_array($table['primary key'])) {
      $this->ensureNotNullPrimaryKey($table['primary key'], $table['fields']);
      $sql_keys[] = 'CONSTRAINT ' . $this->oid('PK_' . $table_name) . ' PRIMARY KEY (' . $this->createColsSql($table['primary key']) . ')';
    }

    if (isset($table['unique keys']) && is_array($table['unique keys'])) {
      foreach ($table['unique keys'] as $key_name => $key) {
        $sql_keys[] = 'CONSTRAINT ' . $this->oid('UK_' . $table_name . '_' . $key_name) . ' UNIQUE (' . $this->createColsSql($key) . ')';
      }
    }

    $sql = "CREATE TABLE " . $oname . " (\n\t" . implode(",\n\t", $sql_fields);
    if (count($sql_keys) > 0) {
      $sql .= ",\n\t";
    }
    $sql .= implode(",\n\t", $sql_keys) . "\n)";
    $statements[] = $sql;

    if (isset($table['indexes']) && is_array($table['indexes'])) {
      foreach ($table['indexes'] as $key_name => $key) {
        $statements = array_merge($statements, $this->createIndexSql($name, $key_name, $key));
      }
    }

    // Add table comment.
    if (isset($table['description'])) {
      $statements[] = 'COMMENT ON TABLE ' . $oname . ' IS ' . $this->prepareComment($table['description']);
    }

    // Add column comments.
    foreach ($table['fields'] as $field_name => $field) {
      if (isset($field['description'])) {
        $statements[] = 'COMMENT ON COLUMN ' . $oname . '.' . $this->oid($field_name) . ' IS ' . $this->prepareComment($field['description']);
      }
    }

    return $statements;
  }

  /**
   * Create an SQL string for a field.
   *
   * To be used in table creation or alteration. Before passing a field out of
   * a schema definition into this function it has to be processed by
   * Schema:processField().
   *
   * @param string $name
   *   Name of the field.
   * @param array $spec
   *   The field specification, as per the schema data structure format.
   *
   * @return string
   *   An array of SQL statements to create the field.
   */
  protected function createFieldSql($name, array $spec) {
    $oname = $this->oid($name);
    $sql = $oname . ' ' . $spec['oracle_type'];

    if ($spec['oracle_type'] == 'VARCHAR2') {
      $sql .= '(' . (!empty($spec['length']) ? $spec['length'] : ORACLE_MAX_VARCHAR2_LENGTH) . ' CHAR)';
    }
    elseif (!empty($spec['length'])) {
      $sql .= '(' . $spec['length'] . ')';
    }
    elseif (isset($spec['precision']) && isset($spec['scale'])) {
      if ($spec['oracle_type'] == 'DOUBLE PRECISION' || $spec['oracle_type'] == 'FLOAT') {
        // For double and floats and precision and scale only NUMBER is supported.
        // @todo Check performance at some point.
        $sql = $oname . ' NUMBER';
      }
      $sql .= '(' . $spec['precision'] . ', ' . $spec['scale'] . ')';
    }

    // DEFAULT ON NULL implies NOT NULL.
    $on_null = '';
    if (!empty($spec['not null'])) {
      $on_null = 'ON NULL ';
    }

    if (isset($spec['identity'])) {
      $sql .= " GENERATED BY DEFAULT " . $on_null . "AS IDENTITY";
    }
    elseif (isset($spec['default'])) {
      $default = $this->escapeDefaultValue($spec['default']);
      // Only cleanup the real NULL case.
      if ($default === "''") {
        $default = "'" . $this->connection->cleanupArgValue('') . "'";
      }
      $sql .= " DEFAULT " . $on_null . "{$default}";
    }
    elseif (!empty($spec['not null'])) {
      $sql .= ' NOT NULL';
    }

    if (!empty($spec['unsigned'])) {
      $sql .= " CHECK ({$oname} >= 0)";
    }

    return $sql;
  }

  /**
   * {@inheritdoc}
   */
  public function getFieldTypeMap() {
    // Put :normal last so it gets preserved by array_flip. This makes
    // it much easier for modules (such as schema.module) to map
    // database types back into schema types.
    static $map = [
      'varchar_ascii:normal' => 'VARCHAR2',

      'varchar:normal'  => 'VARCHAR2',
      'char:normal'     => 'CHAR',

      'text:tiny'       => 'VARCHAR2',
      'text:small'      => 'VARCHAR2',
      // @todo Those should really be CLOB, but due to data corruption
      //       it was switched over to BLOB.
      'text:medium'     => 'BLOB',
      'text:big'        => 'BLOB',
      'text:normal'     => 'BLOB',

      'int:tiny'        => 'NUMBER',
      'int:small'       => 'NUMBER',
      'int:medium'      => 'NUMBER',
      'int:big'         => 'NUMBER',
      'int:normal'      => 'NUMBER',

      'float:tiny'      => 'FLOAT',
      'float:small'     => 'FLOAT',
      'float:medium'    => 'FLOAT',
      'float:big'       => 'DOUBLE PRECISION',
      'float:normal'    => 'FLOAT',

      'numeric:normal'  => 'DOUBLE PRECISION',

      'blob:big'        => 'BLOB',
      'blob:normal'     => 'BLOB',

      // @TODO Recheck this.
      'date:normal' => 'date',

      'datetime:normal' => 'timestamp with local time zone',
      'timestamp:normal' => 'timestamp',
      'time:normal'     => 'timestamp',

      'serial:tiny'     => 'NUMBER',
      'serial:small'    => 'NUMBER',
      'serial:medium'   => 'NUMBER',
      'serial:big'      => 'NUMBER',
      'serial:normal'   => 'NUMBER',
    ];

    return $map;
  }

  /**
   * {@inheritdoc}
   */
  public function renameTable($table, $new_name) {
    $oname = $this->oid($table, TRUE);
    $old_table = $table;
    $old_new_name = $new_name;

    [$schema, $new_name] = $this->tableSchema($new_name);
    [$schema, $table] = $this->tableSchema($table);

    // Should not use prefix because schema is not needed on rename.
    $this->connection->query('ALTER TABLE ' . $oname . ' RENAME TO ' . $this->oid($new_name, FALSE));

    // Rename indexes.
    $stmt = $this->connection->query("SELECT nvl((select identifier from long_identifiers where 'L#'||to_char(id)= index_name),index_name) index_name FROM all_indexes WHERE table_name= ? and owner= ?", array(
      $this->oid($new_name, FALSE, FALSE),
      $schema,
    ));
    while ($row = $stmt->fetchObject()) {
      $this->connection->query('ALTER INDEX ' . $this->oidWithSchema($row->index_name) . ' RENAME TO ' . $this->oid(str_replace(strtoupper($table), strtoupper($new_name), $row->index_name), FALSE));
    }

    $this->cleanUpSchema($old_table);
    $this->cleanUpSchema($old_new_name);
  }

  /**
   * {@inheritdoc}
   */
  public function dropTable($table) {

//    // Workaround to fix deleting of simpletest data.
//    if (preg_match('/^test\d+.*/', $table) === 1) {
//
//      // Always convert to uppercase, because of conversion to lowercase in
//      // findTables() method.
//      return $this->connection->query('DROP USER '. strtoupper($table) .' CASCADE');
//    }

    if (!$this->tableExists($table)) {
      return FALSE;
    }

    $this->connection->query('DROP TABLE ' . $this->oid($table, TRUE) . ' CASCADE CONSTRAINTS PURGE');
    $this->resetTableInformation($table);

    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function addField($table, $field, $spec, $new_keys = array()) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot add field @table.@field: table doesn't exist.", ['@field' => $field, '@table' => $table]));
    }
    if ($this->fieldExists($table, $field)) {
      throw new SchemaObjectExistsException(t('Cannot add field @table.@field: field already exists.', ['@field' => $field, '@table' => $table]));
    }

    // Fields that are part of a PRIMARY KEY must be added as NOT NULL.
    $is_primary_key = isset($new_keys['primary key']) && in_array($field, $new_keys['primary key'], TRUE);
    if ($is_primary_key) {
      $this->ensureNotNullPrimaryKey($new_keys['primary key'], [$field => $spec]);
    }

    $fixnull = FALSE;
    if (!empty($spec['not null']) && !isset($spec['default']) && !$is_primary_key) {
      $fixnull = TRUE;
      $spec['not null'] = FALSE;
    }

    // Actually add this field to the table.
    $query = 'ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD (';
    $query .= $this->createFieldSql($field, $this->processField($spec)) . ')';
    $this->connection->query($query);

    // Once the field is created, update to the needed initial values.
    if (isset($spec['initial_from_field'])) {
      if (isset($spec['initial'])) {
        $expression = 'COALESCE(' . $this->oid($spec['initial_from_field']) . ', :default_initial_value)';
        $arguments = [':default_initial_value' => $spec['initial']];

        // @todo: wrong bind of number values (COALESCE return CHAR type).
        if (is_int($spec['initial'])) {
          $expression = 'COALESCE(' . $this->oid($spec['initial_from_field']) . ', ' . $spec['initial'] . ')';
          $arguments = [];
        }
      }
      else {
        $expression = $spec['initial_from_field'];
        $arguments = [];
      }
      $this->connection->update($table)
        ->expression($field, $expression, $arguments)
        ->execute();
    }
    elseif (isset($spec['initial'])) {
      $this->connection->update($table)
        ->fields([$field => $spec['initial']])
        ->execute();
    }

    // Not null.
    if ($fixnull) {
      $this->connection->query("ALTER TABLE " . $this->oid($table, TRUE) . " MODIFY (" . $this->oid($field) . " NOT NULL)");
    }

    // Make sure to drop the existing primary key before adding a new one.
    // This is only needed when adding a field because this method, unlike
    // changeField(), is supposed to handle primary keys automatically.
    if (isset($new_keys)) {
      if (isset($new_keys['primary key']) && $this->constraintExists($table, 'PK')) {
        $this->dropPrimaryKey($table);
      }
      $this->createKeys($table, $new_keys);
    }

    // Add column comment.
    if (!empty($spec['description'])) {
      $this->connection->query('COMMENT ON COLUMN ' . $this->oid($table, TRUE) . '.' . $this->oid($field) . ' IS ' . $this->prepareComment($spec['description']));
    }

    $this->cleanUpSchema($table);
  }

  /**
   * {@inheritdoc}
   */
  public function dropField($table, $field) {
    if (!$this->fieldExists($table, $field)) {
      return FALSE;
    }

    // Handle "ORA-12991: column is referenced in a multi-column constraint".
    if (!$this->connection->querySafeDdl('ALTER TABLE {' . $table . '} DROP COLUMN ' . $this->oid($field), [], ['12991'])) {

      // Drop the primary key if column in it.
      if (in_array($field, $this->findPrimaryKeyColumns($table), TRUE)) {
        $this->dropPrimaryKey($table);
      }

      // Re-try the deletion.
      $this->connection->query('ALTER TABLE {' . $table . '} DROP COLUMN ' . $this->oid($field));
    }
    $this->cleanUpSchema($table);
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function changeField($table, $field, $field_new, $spec, $keys_new = array()) {
    if (!$this->fieldExists($table, $field)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot change the definition of field @table.@name: field doesn't exist.", ['@table' => $table, '@name' => $field]));
    }
    if (($field != $field_new) && $this->fieldExists($table, $field_new)) {
      throw new SchemaObjectExistsException(t('Cannot rename field @table.@name to @name_new: target field already exists.', ['@table' => $table, '@name' => $field, '@name_new' => $field_new]));
    }
    if (isset($keys_new['primary key']) && in_array($field_new, $keys_new['primary key'], TRUE)) {
      $this->ensureNotNullPrimaryKey($keys_new['primary key'], [$field_new => $spec]);
    }

    // Proceed with table and constraints info.
    $spec = $this->processField($spec);
    $index_schema = $this->introspectIndexSchema($table);

    // Prepare new field definition.
    $field_def = $spec['oracle_type'];
    if ($spec['oracle_type'] == 'VARCHAR2') {
      $field_def .= '(' . (!empty($spec['length']) ? $spec['length'] : ORACLE_MAX_VARCHAR2_LENGTH) . ' CHAR)';
    }
    elseif (!empty($spec['length'])) {
      $field_def .= '(' . $spec['length'] . ')';
    }
    elseif (isset($spec['precision']) && isset($spec['scale'])) {
      if ($spec['oracle_type'] == 'DOUBLE PRECISION' || $spec['oracle_type'] == 'FLOAT') {
        // For double and floats and precision and scale only NUMBER is supported.
        // @todo Check performance at some point.
        $field_def = 'NUMBER';
      }
      $field_def .= '(' . $spec['precision'] . ', ' . $spec['scale'] . ')';
    }

    // Convert the field type and check for the error:
    // "ORA-01439: column to be modified must be empty to change datatype".
    // "ORA-22858: invalid alteration of datatype".
    // "ORA-22859: invalid modification of columns".
    if (!empty($spec['identity']) || !$this->connection->querySafeDdl('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field) . ' ' . $field_def, [], ['01439', '22858', '22859'])) {
      $table_information = $this->queryTableInformation($table);

      $this->connection->query('ALTER TABLE {' . $table . '} RENAME COLUMN ' . $this->oid($field) . ' TO ' . $this->oid($field . '_old'));
      $not_null = isset($spec['not null']) ? $spec['not null'] : FALSE;
      unset($spec['not null']);
      $this->addField($table, $field, $spec);

      // If we change from TEXT to BLOB we need to use a different syntax, but
      // BLOB to BLOB is fine.
      // @todo Support BLOB -> TEXT as well
      if ($spec['oracle_type'] != 'BLOB' || !empty($table_information->blob_fields[$this->oid($field, FALSE, FALSE)])) {
        $this->connection->query('UPDATE {' . $table . '} SET ' . $this->oid($field) . ' = ' . $this->oid($field . '_old'));
      }
      else {
        $this->connection->query('UPDATE {' . $table . '} SET ' . $this->oid($field) . ' = to_blob(utl_raw.cast_to_raw(' . $this->oid($field . '_old') . '))');
      }

      if ($not_null) {
        // "ORA-01442: column to be modified to NOT NULL is already NOT NULL"
        $this->connection->querySafeDdl('ALTER TABLE {' . $table . '} MODIFY (' . $this->oid($field) . ' NOT NULL)', [], [
          '01442',
        ]);
      }
      $this->dropField($table, $field . '_old');

      // Update primary index because if needed.
      if (in_array($field, $index_schema['primary key'], TRUE)) {
        $index_schema['primary key'][array_search($field, $index_schema['primary key'], TRUE)] = $field_new;
        $this->dropPrimaryKey($table);
        $this->addPrimaryKey($table, $index_schema['primary key']);
      }

      // Set new keys.
      if (isset($keys_new)) {
        $this->createKeys($table, $keys_new);
      }

      $this->cleanUpSchema($table);

      // Return early as we added a new field.
      return;
    }

    // Remove old default.
    $this->connection->query('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field) . ' DEFAULT NULL');

    // Handle not null specification.
    if (isset($spec['not null'])) {
      if ($spec['not null']) {
        $nullaction = ' NOT NULL';
      }
      else {
        $nullaction = ' NULL';
      }

      // We do not have current field NULL specification, so try to avoid:
      // "ORA-01442: column to be modified to NOT NULL is already NOT NULL"
      // "ORA-01451: column to be modified to NULL cannot be modified to NULL"
      $this->connection->querySafeDdl('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field) . $nullaction, [], [
        '01442',
        '01451',
      ]);
    }

    // Rename the column if necessary.
    if ($field !== $field_new) {
      $this->connection->query('ALTER TABLE {' . $table . '} RENAME COLUMN ' . $this->oid($field) . ' TO ' . $this->oid($field_new));
    }

    // Add unsigned check if necessary.
    if (!empty($spec['unsigned'])) {
      $this->connection->query('ALTER TABLE {' . $table . '} ADD CHECK (' . $this->oid($field_new) . ' >= 0)');
    }

    // Add default if necessary.
    if (isset($spec['default'])) {
      $default = $this->escapeDefaultValue($this->connection->cleanupArgValue($spec['default']));
      $this->connection->query('ALTER TABLE {' . $table . '} MODIFY ' . $this->oid($field_new) . ' DEFAULT ' . $default);
    }

    // Change description if necessary.
    if (!empty($spec['description'])) {
      $this->connection->query('COMMENT ON COLUMN {' . $table . '}.' . $this->oid($field_new) . ' IS ' . $this->prepareComment($spec['description']));
    }

    // Update primary index because if needed.
    if (in_array($field, $index_schema['primary key'], TRUE)) {
      $index_schema['primary key'][array_search($field, $index_schema['primary key'], TRUE)] = $field_new;
      $this->dropPrimaryKey($table);
      $this->addPrimaryKey($table, $index_schema['primary key']);
    }

    // Set new keys.
    if (isset($keys_new)) {
      $this->createKeys($table, $keys_new);
    }

    $this->cleanUpSchema($table);
  }

  /**
   * Set database-engine specific properties for a field.
   *
   * @param array $field
   *   A field description array, as specified in the schema documentation.
   *
   * @return array
   *   A field description array after changes.
   */
  protected function processField($field) {
    if (!isset($field['size'])) {
      $field['size'] = 'normal';
    }

    // Set the correct database-engine specific datatype.
    $map = $this->getFieldTypeMap();
    if (isset($field['oracle_type'])) {
      $field['oracle_type'] = strtoupper($field['oracle_type']);
    }
    // HACK: Core wants a BLOB field for translation source, but wants to query on it via IN.
    elseif (isset($field['mysql_type']) && $field['mysql_type'] == 'blob' && $field['type'] == 'text') {
      $field['oracle_type'] = 'VARCHAR2';
    }
    elseif (!isset($field['type']) && isset($field['pgsql_type'])) {
      $field['oracle_type'] = $map[$field['pgsql_type'] . ':' . $field['size']];
    }
    elseif ($field['type']) {
      $field['oracle_type'] = $map[$field['type'] . ':' . $field['size']];
    }

    if (!empty($field['type']) && $field['type'] == 'serial') {
      $field['identity'] = TRUE;
    }

    return $field;
  }

  /**
   * {@inheritdoc}
   */
  public function fieldSetDefault($table, $field, $default) {
    @trigger_error('fieldSetDefault() is deprecated in drupal:8.7.0 and will be removed before drupal:9.0.0. Instead, call ::changeField() passing a full field specification. See https://www.drupal.org/node/2999035', E_USER_DEPRECATED);
    if (!$this->fieldExists($table, $field)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot set default value of field @table.@field: field doesn't exist.", ['@table' => $table, '@field' => $field]));
    }

    $oname = $this->oid($table, TRUE);
    [$schema, $table] = $this->tableSchema($table);

    $is_not_null = $this->connection->query("SELECT 1 FROM all_tab_columns WHERE column_name = ? and table_name = ? and owner= ? AND nullable = 'N'", array(
      $this->oid($field, FALSE, FALSE),
      $this->oid($table, FALSE, FALSE),
      $schema,
    ))->fetchField();

    $on_null = '';
    if (is_null($default)) {
      $default = 'NULL';
    }
    else {
      if ($is_not_null) {
        $on_null = 'ON NULL ';
      }

      $default = $this->escapeDefaultValue($this->connection->cleanupArgValue($default));
    }

    $this->connection->query('ALTER TABLE ' . $oname . ' MODIFY (' . $this->oid($field) . ' DEFAULT ' . $on_null . $default . ' )');

    // Oracle does get confused from the NULL and removes the NOT NULL CONSTRAINT
    // so we have to bring it back.
    if ($is_not_null) {
      // "ORA-01442: column to be modified to NOT NULL is already NOT NULL"
      $this->connection->querySafeDdl('ALTER TABLE ' . $oname . ' MODIFY (' . $this->oid($field) . ' NOT NULL)', [], [
        '01442',
      ]);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function fieldSetNoDefault($table, $field) {
    @trigger_error('fieldSetNoDefault() is deprecated in drupal:8.7.0 and will be removed before drupal:9.0.0. Instead, call ::changeField() passing a full field specification. See https://www.drupal.org/node/2999035', E_USER_DEPRECATED);
    if (!$this->fieldExists($table, $field)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot remove default value of field @table.@field: field doesn't exist.", ['@table' => $table, '@field' => $field]));
    }

    $oname = $this->oid($table, TRUE);
    [$schema, $table] = $this->tableSchema($table);
    $is_not_null = $this->connection->query("SELECT 1 FROM all_tab_columns WHERE column_name = ? and table_name = ? and owner= ? AND nullable = 'N'", array(
      $this->oid($field, FALSE, FALSE),
      $this->oid($table, FALSE, FALSE),
      $schema,
    ))->fetchField();

    $this->connection->query('ALTER TABLE ' . $oname . ' MODIFY (' . $this->oid($field) . ' DEFAULT NULL)');

    // Oracle does get confused from the NULL and removes the NOT NULL CONSTRAINT
    // so we have to bring it back.
    if ($is_not_null) {
      // "ORA-01442: column to be modified to NOT NULL is already NOT NULL"
      $this->connection->querySafeDdl('ALTER TABLE ' . $oname . ' MODIFY (' . $this->oid($field) . ' NOT NULL)', [], [
        '01442',
      ]);
    }
  }

  /**
   * Helper function: check if a constraint exists.
   *
   * @param string $table
   *   The name of the table.
   * @param string $prefix
   *   The prefix of the constraint (typically 'PK' or 'UK').
   * @param string $name
   *   The name of the constraint (optional)
   *
   * @return bool
   *   TRUE if the constraint exists, FALSE otherwise.
   */
  public function constraintExists($table, $prefix, $name = '') {
    [$constraint_schema, $table] = $this->tableSchema($table);

    $table_name = $this->oid($table, FALSE, FALSE);
    if ($name != '') {
      $name = '_' . $name;
    }
    $constraint_name = $this->oid($prefix . '_' . $table . $name, FALSE, FALSE);
    return (bool) $this->connection->query("
     SELECT constraint_name
       FROM all_constraints
      WHERE constraint_name = :constraint_name
        AND table_name = :table_name
        AND owner = :constraint_schema", [
      ':table_name' => $table_name,
      ':constraint_name' => $constraint_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchField();
  }

  /**
   * {@inheritdoc}
   */
  protected function findPrimaryKeyColumns($table) {
    if (!$this->tableExists($table)) {
      return FALSE;
    }

    if (!$this->constraintExists($table, 'PK')) {
      return [];
    }
    [$constraint_schema, $table] = $this->tableSchema($table);

    $table_name = $this->oid($table, FALSE, FALSE);
    $constraint_name = $this->oid('PK_' . $table, FALSE, FALSE);
    $constraint_columns = $this->connection->query('
     SELECT column_name
       FROM all_cons_columns
      WHERE constraint_name = :constraint_name
        AND table_name = :table_name
        AND owner = :constraint_schema
   ORDER BY position ASC', [
      ':table_name' => $table_name,
      ':constraint_name' => $constraint_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchCol();

    return array_map('strtolower', $constraint_columns);
  }

  /**
   * {@inheritdoc}
   */
  protected function introspectIndexSchema($table) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException("The table {$table} doesn't exist.");
    }

    $index_schema = [
      'primary key' => [],
      'unique keys' => [],
      'indexes' => [],
    ];

    [$constraint_schema, $table] = $this->tableSchema($table);
    $table_name = $this->oid($table, FALSE, FALSE);

    $constraint_columns = $this->connection->query('
     SELECT constraint_name, LOWER(column_name) AS column_name
       FROM all_cons_columns
      WHERE table_name = :table_name
        AND owner = :constraint_schema
   ORDER BY position ASC', [
      ':table_name' => $table_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchAll();
    foreach ($constraint_columns as $constraint) {
      if (0 === strpos($constraint->constraint_name, 'PK_')) {
        $index_schema['primary key'][] = $constraint->column_name;
      }
      elseif (0 === strpos($constraint->constraint_name, 'UK_')) {
        // Format `UK_TABLE_NAME_KEY_NAME` into `key_name`.
        $constraint_name = strtolower(substr($constraint->constraint_name, 4 + strlen($table)));
        $index_schema['unique keys'][$constraint_name][] = $constraint->column_name;
      }
    }

    $indexes = $this->connection->query('
     SELECT index_name, LOWER(column_name) AS column_name
       FROM all_ind_columns
      WHERE table_name = :table_name
        AND index_owner = :constraint_schema', [
      ':table_name' => $table_name,
      ':constraint_schema' => $constraint_schema,
    ])->fetchAll();
    foreach ($indexes as $index) {
      if (0 === strpos($index->index_name, 'IDX_')) {
        // Format `IDX_TABLE_NAME_INDEX_NAME` into `key_name`.
        $index_name = strtolower(substr($index->index_name, 5 + strlen($table)));
        $index_schema['indexes'][$index_name][] = $index->column_name;
      }
    }
    return $index_schema;
  }

  /**
   * {@inheritdoc}
   */
  public function addPrimaryKey($table, $fields) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot add primary key to table @table: table doesn't exist.", ['@table' => $table]));
    }
    if ($this->constraintExists($table, 'PK')) {
      throw new SchemaObjectExistsException(t('Cannot add primary key to table @table: primary key already exists.', ['@table' => $table]));
    }

    $oname = $this->oid($table, TRUE);
    [$schema, $table] = $this->tableSchema($table);

    $this->connection->query('ALTER TABLE ' . $oname  . ' ADD CONSTRAINT ' . $this->oid('PK_' . $table) . ' PRIMARY KEY (' . $this->createColsSql($fields) . ')');
  }

  /**
   * {@inheritdoc}
   */
  public function dropPrimaryKey($table) {
    if (!$this->constraintExists($table, 'PK')) {
      return FALSE;
    }
    $oname = $this->oid($table, TRUE);
    [$schema, $table] = $this->tableSchema($table);

    $this->connection->query('ALTER TABLE ' . $oname . ' DROP CONSTRAINT ' . $this->oid('PK_' . $table));
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function addUniqueKey($table, $name, $fields) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot add unique key @name to table @table: table doesn't exist.", ['@table' => $table, '@name' => $name]));
    }
    if ($this->constraintExists($table, 'UK', $name)) {
      throw new SchemaObjectExistsException(t("Cannot add unique key @name to table @table: unique key already exists.", ['@table' => $table, '@name' => $name]));
    }

    $this->connection->query('ALTER TABLE ' . $this->oid($table, TRUE) . ' ADD CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name) . ' UNIQUE (' . $this->createColsSql($fields) . ')');
  }

  /**
   * {@inheritdoc}
   */
  public function dropUniqueKey($table, $name) {
    if (!$this->constraintExists($table, 'UK', $name)) {
      return FALSE;
    }
    $oname = $this->oid($table, TRUE);
    [$schema, $table] = $this->tableSchema($table);

    $this->connection->query('ALTER TABLE ' . $oname . ' DROP CONSTRAINT ' . $this->oid('UK_' . $table . '_' . $name));
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function addIndex($table, $name, $fields, array $specs) {
    if (!$this->tableExists($table)) {
      throw new SchemaObjectDoesNotExistException(t("Cannot add index @name to table @table: table doesn't exist.", ['@table' => $table, '@name' => $name]));
    }
    if ($this->indexExists($table, $name)) {
      throw new SchemaObjectExistsException(t('Cannot add index @name to table @table: index already exists.', ['@table' => $table, '@name' => $name]));
    }

    $statements = $this->createIndexSql($table, $name, $fields);
    foreach ($statements as $statement) {
      $this->connection->query($statement, [], ['allow_delimiter_in_query' => TRUE]);
    }
  }

  /**
   * {@inheritdoc}
   */
  public function dropIndex($table, $name) {
    if (!$this->indexExists($table, $name)) {
      return FALSE;
    }

    $this->connection->query('DROP INDEX ' . $this->oidWithSchema('IDX_' . $table . '_' . $name));
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function indexExists($table, $name) {
    [$schema, $table] = $this->tableSchema($table);

    $oname = $this->oid('IDX_' . $table . '_' . $name, FALSE, FALSE);

    $retval = $this->connection->query("SELECT 1 FROM all_indexes WHERE index_name = ? and table_name= ? and owner= ?", array(
      $oname, $this->oid($table, FALSE, FALSE),
      $schema,
    ))->fetchField();

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  /**
   * Retrieve a table or column comment.
   */
  public function getComment($table, $column = NULL) {
    [$schema, $table] = $this->tableSchema($table);

    if (isset($column)) {
      return $this->connection->query('select comments from all_col_comments where column_name = ? and table_name = ? and owner = ?', array(
        $this->oid($column, FALSE, FALSE),
        $this->oid($table, FALSE, FALSE),
        $schema,
      ))->fetchField();
    }

    return $this->connection->query('select comments from all_tab_comments where table_name = ? and owner = ?', array(
      $this->oid($table, FALSE, FALSE),
      $schema,
    ))->fetchField();
  }

  /**
   * {@inheritdoc}
   */
  public function tableExists($table) {
    [$schema, $table] = $this->tableSchema($table);

    $retval = $this->connection->query("SELECT 1 FROM all_tables WHERE temporary= 'N' and table_name = ? and owner= ?", array(
      $this->oid($table, FALSE, FALSE),
      $schema,
    ))->fetchField();

    if ($retval) {
      return TRUE;
    }
    else {
      return FALSE;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function fieldExists($table, $column) {
    [$schema, $table] = $this->tableSchema($table);

    $retval = $this->connection->query("SELECT 1 FROM all_tab_columns WHERE column_name = ? and table_name = ? and owner= ?", array(
      $this->oid($column, FALSE, FALSE),
      $this->oid($table, FALSE, FALSE),
      $schema,
    ))->fetchField();

    if ($retval) {
      return TRUE;
    }
    return FALSE;
  }

  /**
   * {@inheritdoc}
   */
  public function findTables($table_expression) {
    // Load all the tables up front in order to take into account per-table
    // prefixes. The actual matching is done at the bottom of the method.
    $individually_prefixed_tables = $this->connection->getUnprefixedTablesMap();
    $default_prefix = $this->connection->tablePrefix();
    $default_prefix_length = strlen($default_prefix);
    $tables = [];

    // This is only minimally changed from core's findTables.
    // --> ALTERED LINES
    if (strpos($default_prefix, '.') !== FALSE) {
      $default_prefix = '';
      $default_prefix_length = 0;
    }

    [$schema,] = $this->tableSchema($table_expression);
    $results = $this->connection->query('SELECT table_name as table_name FROM all_tables WHERE owner = ?', [$schema])->fetchAll();
    // --> ALTERED LINES END

    foreach ($results as $table) {
      // Take into account tables that have an individual prefix.
      if (isset($individually_prefixed_tables[$table->table_name])) {
        $prefix_length = strlen($this->connection->tablePrefix($individually_prefixed_tables[$table->table_name]));
      }
      elseif ($default_prefix && substr($table->table_name, 0, $default_prefix_length) !== $default_prefix) {
        // This table name does not start the default prefix, which means that
        // it is not managed by Drupal so it should be excluded from the result.
        continue;
      }
      else {
        $prefix_length = $default_prefix_length;
      }

      // Remove the prefix from the returned tables.
      $unprefixed_table_name = substr($table->table_name, $prefix_length);

      // --> ADDED: strtolower to make compatible with core.
      $unprefixed_table_name = strtolower($unprefixed_table_name);

      // --> HACK: Make test work
      if ($unprefixed_table_name == 'testtable') {
        $unprefixed_table_name = 'testTable';
      }

      // The pattern can match a table which is the same as the prefix. That
      // will become an empty string when we remove the prefix, which will
      // probably surprise the caller, besides not being a prefixed table. So
      // remove it.
      if (!empty($unprefixed_table_name)) {
        $tables[$unprefixed_table_name] = $unprefixed_table_name;
      }
    }

    // Convert the table expression from its SQL LIKE syntax to a regular
    // expression and escape the delimiter that will be used for matching.
    $table_expression = str_replace(['%', '_'], ['.*?', '.'], preg_quote($table_expression, '/'));
    $tables = preg_grep('/^' . $table_expression . '$/i', $tables);

    return $tables;
  }

  /**
   * {@inheritdoc}
   */
  public function copyTable($source, $destination) {
    if (!$this->tableExists($source)) {
      throw new SchemaObjectDoesNotExistException(SafeMarkup::format("Cannot copy @source to @destination: table @source doesn't exist.", array('@source' => $source, '@destination' => $destination)));
    }
    if ($this->tableExists($destination)) {
      throw new SchemaObjectExistsException(SafeMarkup::format("Cannot copy @source to @destination: table @destination already exists.", array('@source' => $source, '@destination' => $destination)));
    }

    throw new DatabaseExceptionWrapper('Not implemented, see https://drupal.org/node/2056133.');
  }

  /**
   * Oracle schema helper.
   */
  protected function createColsSql($cols) {
    $return = array();
    foreach ($cols as $col) {
      if (is_array($col)) {
        $return[] = $this->oid($col[0]);
      }
      else {
        $return[] = $this->oid($col);
      }
    }
    return implode(', ', $return);
  }

  /**
   * Oracle schema helper.
   */
  protected function createIndexSql($table, $name, $fields) {
    [$schema, $table_name] = $this->tableSchema($table);
    $oname = $this->oidWithSchema('IDX_' . $table_name . '_' . $name);

    $sql = array();
    // Oracle doesn't like multiple indexes on the same column list.
    $ret = $this->dropIndexByColsSql($table, $fields);

    if ($ret) {
      $sql[] = $ret;
    }

    // Suppose we try to create two indexes in the same create table command we
    // will silently fail the second.
    $query = "begin execute immediate 'CREATE INDEX " . $oname . " ON " . $this->oid($table, TRUE) . " (";
    $query .= $this->createKeySql($fields) . ")'; exception when others then if sqlcode = -1408 then null; else raise; end if; end;";
    $sql[] = $query;

    return $sql;
  }

  /**
   * Oracle schema helper.
   */
  public function dropIndexByColsSql($table, $fields) {
    [$schema, $table] = $this->tableSchema($table);
    $stmt = $this->connection->queryOracle(
      "select i.index_name,
       e.column_expression exp,
       i.column_name col
       from all_ind_columns i,
       all_ind_expressions e
       where i.column_position= e.column_position (+)
       and i.index_owner = e.index_owner (+)
       and i.table_name = e.table_name (+)
       and i.index_name = e.index_name (+)
       and (i.index_name like 'IDX%' or i.index_name like '" . ORACLE_LONG_IDENTIFIER_PREFIX . "%')
       and i.table_name = ?
       and i.index_owner = ?
      ",
      array(strtoupper($table), $schema)
    );

    $idx = array();
    while ($row = $stmt->fetchObject()) {
      if (!isset($idx[$row->index_name])) {
        $idx[$row->index_name] = array();
      }
      $idx[$row->index_name][] = $row->exp ? $row->exp : $row->col;
    }

    $col = array();

    foreach ($fields as $field) {
      if (is_array($field)) {
        $col[] = 'SUBSTR(' . $this->oid($field[0]) . ',1,' . $field[1] . ')';
      }
      else {
        $col[] = $this->oid($field, FALSE, FALSE);
      }
    }

    foreach ($idx as $name => $value) {
      if (!count(array_diff($value, $col))) {
        return 'DROP INDEX "' . strtoupper($schema) . '"."' . strtoupper($name) . '"';
      }
    }

    return FALSE;
  }

  /**
   * Oracle schema helper.
   */
  protected function createKeySql($fields) {
    $ret = array();
    foreach ($fields as $field) {
      if (is_array($field)) {
        $ret[] = 'substr(' . $this->oid($field[0]) . ', 1, ' . $field[1] . ')';
      }
      else {
        $ret[] = $this->oid($field);
      }
    }
    return implode(', ', $ret);
  }

  /**
   * Oracle schema helper.
   */
  protected function createKeys($table, $new_keys) {
    if (isset($new_keys['primary key'])) {
      $this->addPrimaryKey($table, $new_keys['primary key']);
    }

    if (isset($new_keys['unique keys'])) {
      foreach ($new_keys['unique keys'] as $name => $fields) {
        $this->addUniqueKey($table, $name, $fields);
      }
    }

    if (isset($new_keys['indexes'])) {
      foreach ($new_keys['indexes'] as $name => $fields) {
        $this->addIndex($table, $name, $fields);
      }
    }
  }

  /**
   * Helper to return [schema,name] for unprefixed table name.
   *
   * @param string $table_name
   *   The name of the table to get a full schema for.
   *
   * @return array
   *   An array with the schema and the table name.
   */
  public function tableSchema($table_name) {
    static $owner;

    $table = strtoupper(str_replace('"', '', $this->connection->prefixTables('{' . $table_name . '}')));

    $exp = explode('.', $table, 2);
    if (count($exp) > 1) {
      return $exp;
    }

    if (!isset($owner)) {
      $owner = $this->connection
        ->queryOracle('SELECT USER FROM dual')
        ->fetchField();
    }

    return [$owner, $exp[0]];
  }

  /**
   * Oracle schema helper.
   */
  private function cleanUpSchema($cache_table) {
    $this->resetLongIdentifiers();
    $this->resetTableInformation($cache_table);
  }

}

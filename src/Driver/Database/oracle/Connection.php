<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\IntegrityConstraintViolationException;
use Drupal\Core\Database\Database;
use Drupal\Core\Database\Connection as DatabaseConnection;
use Drupal\Core\Database\Log;
use Drupal\Core\Database\StatementInterface;
use Drupal\oracle\Driver\Database\oracle\StatementWrapper;

/**
 * Used to replace '' character in queries.
 */
define('ORACLE_EMPTY_STRING_REPLACER', '^');

/**
 * Maximum oracle identifier length (e.g. table names cannot exceed the length).
 *
 * @TODO: make dynamic. 30 is a limit for v11. In OD12+ has new limit of 128.
 * Current value can be get by `DESCRIBE all_tab_columns`.
 */
define('ORACLE_IDENTIFIER_MAX_LENGTH', 128);

/**
 * Prefix used for long identifier keys.
 */
define('ORACLE_LONG_IDENTIFIER_PREFIX', 'L#');

/**
 * Maximum length (in bytes) for a string value in a table column in oracle.
 *
 * Affects schema.inc table creation.
 */
define('ORACLE_MAX_VARCHAR2_LENGTH', 4000);

/**
 * @addtogroup database
 * @{
 */

/**
 * Oracle implementation of \Drupal\Core\Database\Connection.
 */
class Connection extends DatabaseConnection {

  protected $oracleReservedWords = [
    'ACCESS',
    'ADD',
    'ALL',
    'ALTER',
    'AND',
    'ANY',
    'AS',
    'ASC',
    'AUDIT',
    'BETWEEN',
    'BY',
    'CHAR',
    'CHECK',
    'CLUSTER',
    'COLUMN',
    'COLUMN_VALUE',
    'COMMENT',
    'COMPRESS',
    'CONNECT',
    'CREATE',
    'CURRENT',
    'DATE',
    'DECIMAL',
    'DEFAULT',
    'DELETE',
    'DESC',
    'DISTINCT',
    'DROP',
    'ELSE',
    'EXCLUSIVE',
    'EXISTS',
    'FILE',
    'FLOAT',
    'FOR',
    'FROM',
    'GRANT',
    'GROUP',
    'HAVING',
    'IDENTIFIED',
    'IMMEDIATE',
    'IN',
    'INCREMENT',
    'INDEX',
    'INITIAL',
    'INSERT',
    'INTEGER',
    'INTERSECT',
    'INTO',
    'IS',
    'LEVEL',
    'LIKE',
    'LOCK',
    'LONG',
    'MAXEXTENTS',
    'MINUS',
    'MLSLABEL',
    'MODE',
    'MODIFY',
    'NESTED_TABLE_ID',
    'NOAUDIT',
    'NOCOMPRESS',
    'NOT',
    'NOWAIT',
    'NULL',
    'NUMBER',
    'OF',
    'OFFLINE',
    'ON',
    'ONLINE',
    'OPTION',
    'OR',
    'ORDER',
    'PCTFREE',
    'PRIOR',
    'PUBLIC',
    'RAW',
    'RENAME',
    'RESOURCE',
    'REVOKE',
    'ROW',
    'ROWID',
    'ROWNUM',
    'ROWS',
    'SELECT',
    'SESSION',
    'SET',
    'SHARE',
    'SID',
    'SIZE',
    'SMALLINT',
    'START',
    'SUCCESSFUL',
    'SYNONYM',
    'SYSDATE',
    'TABLE',
    'THEN',
    'TO',
    'TRIGGER',
    'UID',
    'UNION',
    'UNIQUE',
    'UPDATE',
    'USER',
    'VALIDATE',
    'VALUES',
    'VARCHAR',
    'VARCHAR2',
    'VIEW',
    'WHENEVER',
    'WHERE',
    'WITH',
  ];

  /**
   * Error code for "Unknown database" error.
   */
  const DATABASE_NOT_FOUND = 0;

  /**
   * We are being use to connect to an external oracle database.
   *
   * @var bool
   */
  public $external = FALSE;

  /**
   * {@inheritdoc}
   */
  protected $statementClass = NULL;

  /**
   * {@inheritdoc}
   */
  protected $statementWrapperClass = StatementWrapper::class;

   /**
    * {@inheritdoc}
    */
  public function __construct(\PDO $connection, array $connection_options = array()) {
    parent::__construct($connection, $connection_options);

    // This driver defaults to transaction support, except if explicitly
    // passed FALSE.
    $this->transactionSupport = !isset($connection_options['transactions']) || ($connection_options['transactions'] !== FALSE);

    // Transactional DDL is not available in Oracle.
    $this->transactionalDDLSupport = FALSE;

    // Setup session attributes.
    try {
      $stmt = $this->prepareStatement("SELECT setup_session() FROM dual", []);
      $stmt->execute();
    }
    catch (\Exception $ex) {
      // Connected to an external oracle database (not necessarily a drupal
      // schema).
      $this->external = TRUE;
    }

    // Execute Oracle init_commands.
    if (isset($connection_options['init_commands'])) {
      $this->connection->exec(implode('; ', $connection_options['init_commands']));
    }

    // Ensure all used Oracle prefixes (users schemas) exists.
    foreach ($this->prefixes as $table_name => $prefix) {
      if (!empty($prefix) && strpos($prefix, '.') !== FALSE) {
        $prefix = str_replace(['"', '.'],['',''], $prefix);

        // This will create the user if not exists.
        // @todo: clean up Simpletest TEST% users.

        // Allow ORA-00904 ("invalid identifier error") and ORA-06575 ("package
        // or function is in an invalid state") and during the installation
        // process (before the 'identifier' package were created).
        $this->querySafeDdl('SELECT identifier.check_db_prefix(?) FROM dual', [$prefix], ['06575', '00904']);
      }
    }
  }

  /**
   * {@inheritdoc}
   */
  protected $identifierQuotes = ['"', '"'];

  /**
   * {@inheritdoc}
   */
  public static function open(array &$connection_options = array()) {

    /**
     * Here is full possible TNS description (with master/slave).
     * @todo: implement all options?
     *
     * (DESCRIPTION_LIST=
     *   (LOAD_BALANCE=off)
     *   (FAILOVER=on)
     *   (DESCRIPTION=
     *     (ADDRESS=(PROTOCOL=TCPS)(HOST=<master_host>)(PORT=<master_port>))
     *     (CONNECT_DATA=(SERVICE_NAME=<master_service_name>))
     *     (SECURITY= (SSL_SERVER_CERT_DN="for example cn=sales,cn=OracleContext,dc=us,dc=acme,dc=com"))
     *   )
     *   (DESCRIPTION=
     *     (ADDRESS=(PROTOCOL=TCPS)(HOST=<slave_host>)(PORT=<slave_port>))
     *     (CONNECT_DATA=(SERVICE_NAME=<master_service_name>))
     *     (SECURITY=(SSL_SERVER_CERT_DN="for example cn=sales,cn=OracleContext,dc=us,dc=acme,dc=com"))
     *   )
     * )
     */

    if ($connection_options['host'] === 'USETNS') {
      // Use database as TNSNAME.
      $dsn = 'oci:dbname=' . $connection_options['database'] . ';charset=AL32UTF8';
    }
    else {
      // Use host/port/database.
      $tns = "(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = {$connection_options['host']})(PORT = {$connection_options['port']})) (CONNECT_DATA = (SERVICE_NAME = {$connection_options['database']}) (SID = {$connection_options['database']})))";
      $dsn = "oci:dbname={$tns};charset=AL32UTF8";
    }

    // Allow PDO options to be overridden.
    $connection_options += array(
      'pdo' => array(),
    );

    $connection_options['pdo'] += array(
      \PDO::ATTR_STRINGIFY_FETCHES => TRUE,
      \PDO::ATTR_CASE => \PDO::CASE_LOWER,
      \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
    );

    $pdo = new \PDO($dsn, $connection_options['username'], $connection_options['password'], $connection_options['pdo']);

    return $pdo;
  }

  /**
   * {@inheritdoc}
   */
  protected function setPrefix($prefix) {
    if (!is_array($prefix)) {
      $prefix = ['default' => $prefix];
    }

    if (getenv("ORACLE_RUN_TESTS_IN_EXTRA_DB")) {
      foreach ($prefix as $key => $value) {
        $prefix[$key] = 'C##' . $prefix[$key] . '.';
      }
    }

    foreach ($prefix as $key => $value) {
      $prefix[$key] = strtoupper($prefix[$key]);

      // Ensure database. as prefix is supported.
      if (strpos($prefix[$key], '.') !== FALSE) {
        $prefix[$key] = str_replace('.', '"."', str_replace('"', '', $prefix[$key]));
      }
    }

    return parent::setPrefix($prefix);
  }

  /**
   * {@inheritdoc}
   */
  public function query($query, array $args = [], $options = []) {
    // Use default values if not already set.
    $options += $this->defaultOptions();

    $return_last_id = FALSE;
    if (($options['return'] ?? Database::RETURN_STATEMENT) == Database::RETURN_INSERT_ID) {
      $return_last_id = TRUE;
      unset($options['return']);
    }

    $result = parent::query($query, $args, $options);

    // Override the default RETURN_INSERT_ID.
    if ($return_last_id) {
      return (isset($options['sequence_name']) ? $this->lastInsertId($options['sequence_name']) : FALSE);
    }

    return $result;
  }

  /**
   * {@inheritdoc}
   */
  public function queryRange($query, $from, $count, array $args = array(), array $options = array()) {
    $start = (int) $from;
    $count = (int) $count;

    if ($start == 0) {
      $query .= sprintf(" FETCH FIRST %d ROWS ONLY", (int) $count);
    } else {
      $query .= sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", (int) $from, (int) $count);
    }

    return $this->query($query, $args, $options);
  }

  /**
   * {@inheritdoc}
   */
  public function queryTemporary($query, array $args = array(), array $options = array()) {
    @trigger_error('Connection::queryTemporary() is deprecated in drupal:9.3.0 and is removed from drupal:10.0.0. There is no replacement. See https://www.drupal.org/node/3211781', E_USER_DEPRECATED);

    $tablename = $this->generateTemporaryTableName();
    try {
      $this->query('DROP TABLE {' . $tablename . '}');
    }
    catch (\Exception $ex) {
      /* ignore drop errors */
    }
    $this->query('CREATE GLOBAL TEMPORARY TABLE {' . $tablename . '} ON COMMIT PRESERVE ROWS AS ' . $query, $args, $options);
    return $tablename;
  }

  /**
   * Helper function: allow to ignore some or all ORA errors.
   *
   * @param string $query
   *   The query to execute. In most cases this will be a string containing
   *   an SQL query with placeholders.
   *
   * @param array $args
   *   An array of arguments for the prepared statement. If the prepared
   *   statement uses ? placeholders, this array must be an indexed array.
   *   If it contains named placeholders, it must be an associative array.
   *
   * @param array $allowed
   *   The array of allowed ORA errors.
   *
   * @return bool
   *   FALSE if the error occurs, TRUE if not.
   *
   * @throws \Drupal\Core\Database\DatabaseExceptionWrapper
   */
  public function querySafeDdl($query, $args = [], $allowed = []) {
    try {
      $this->query($query, $args);
    }
    catch (DatabaseExceptionWrapper $exception) {
      // Ignore all errors.
      if (empty($allowed)) {
        return FALSE;
      }

      // Ignore allowed errors.
      if (in_array($exception->getCode(), $allowed, FALSE)) {
        return FALSE;
      }

      // Throw an exception otherwise.
      throw $exception;
    }
    return TRUE;
  }

  /**
   * Executes an internal driver queries query string against the database.
   *
   * @param string $query
   *   The query to execute. In most cases this will be a string containing
   *   an SQL query with placeholders.
   *
   * @param array $args
   *   An array of arguments for the prepared statement. If the prepared
   *   statement uses ? placeholders, this array must be an indexed array.
   *   If it contains named placeholders, it must be an associative array.
   *
   * @return \Drupal\Driver\Database\oracle\Statement
   *
   * @throws \Drupal\Core\Database\DatabaseExceptionWrapper
   */
  public function queryOracle($query, $args = []) {
    // @todo: refactor to use query() method + additional options like
    // `oracle_disable_log` + `oracle_processable_query`.

    try {
      $logger = $this->pauseLog();
      $stmt = new $this->statementWrapperClass($this, $this->connection, $query, []);
      $stmt->execute($args);
      $this->continueLog($logger);
      return $stmt;
    }
    catch (DatabaseExceptionWrapper $exception) {
      syslog(LOG_ERR, "error: {$exception->getMessage()} {$query}");
      throw $exception;
    }
  }

  /**
   * {@inheritdoc}
   */
  public function driver() {
    return 'oracle';
  }

  /**
   * {@inheritdoc}
   */
  public function databaseType() {
    return 'oracle';
  }

  /**
   * {@inheritdoc}
   */
  public function createDatabase($database) {
    // Database can be created manually only.
  }

  /**
   * {@inheritdoc}
   */
  public function mapConditionOperator($operator) {
    // We don't want to override any of the defaults.
    return NULL;
  }

  /**
   * {@inheritdoc}
   */
  public function getFullQualifiedTableName($table) {
    $options = $this->getConnectionOptions();
    [$schema, $table] = $this->schema()->tableSchema($table);

    // The fully qualified table name in Oracle Database is in the form of:
    // <user_name>.<table_name>@<database>. Where <database> is either
    // a database link created via `CREATE DATABASE LINK` or an alias from
    // Local Naming Parameters configuration (by default is located in the
    // $ORACLE_HOME/network/admin/tnsnames.ora). This Driver DO NOT support
    // auto creation of database links for the connection.
    return $schema . '.' . $table . '@' . $options['database'];
  }

  /**
   * {@inheritdoc}
   */
  public function escapeTable($table) {
    if (!isset($this->escapedTables[$table])) {
      $this->escapedTables[$table] = preg_replace('/[^A-Za-z0-9_.@#]+/', '', $table);
    }
    return $this->escapedTables[$table];
  }

  /**
   * {@inheritdoc}
   */
  public function nextId($existing_id = 0) {
    // Retrieve the name of the sequence. This information cannot be cached
    // because the prefix may change, for example, like it does in simpletests.
    $table_information = $this->schema()->queryTableInformation('sequences');
    $sequence_name = $table_information->sequences[0];

    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();
    if ($id > $existing_id) {
      return $id;
    }

    $new_id = ((int) $existing_id) + 1;

    // Reset the sequence to a higher value than the existing id.
    $this->query("ALTER TABLE {sequences} MODIFY (value GENERATED BY DEFAULT ON NULL AS IDENTITY START WITH $new_id)");

    // Retrive the next id. We know this will be as high as we want it.
    $id = $this->query("SELECT " . $sequence_name . ".currval FROM DUAL")->fetchField();
    $id = $this->query("SELECT " . $sequence_name . ".nextval FROM DUAL")->fetchField();

    return $id;
  }

  /**
   * Oracle connection helper.
   */
  public function makePrimary() {
    // We are installing a primary database.
    $this->external = FALSE;
  }

 /**
   * Oracle connection helper.
   */
  public function isPrimary() {
    return !$this->external;
  }

  /**
   * Pause the database logging if any available.
   *
   * @see \Drupal\Core\Database::startLog()
   *
   * @return \Drupal\Core\Database\Log|null
   *  An instance of the database logger, or NULL if logging was not started.
   */
  public function pauseLog()  {
    if (!empty($this->logger)) {
      $logger = $this->logger;
      $this->logger = NULL;
      return $logger;
    }

    return NULL;
  }

  /**
   * Continue the previously paused database logger.
   *
   * @param $logger \Drupal\Core\Database\Log|null
   *  An instance of the database logger, or NULL if logging was not started.
   *
   * @see \Drupal\Driver\Database\oracle\Connection::pauseLog()
   */
  public function continueLog($logger) {
    if ($logger instanceof Log) {
      $this->logger = $logger;
    }
  }

  /**
   * Oracle connection helper.
   */
  private function exceptionQuery(&$unformattedQuery) {
    global $_oracle_exception_queries;

    if (!is_array($_oracle_exception_queries)) {
      return FALSE;
    }

    $count = 0;
    $oracle_unformatted_query = preg_replace(
      array_keys($_oracle_exception_queries),
      array_values($_oracle_exception_queries),
      $oracle_unformatted_query,
      -1,
      $count
    );

    return $count;
  }

  /**
   * Oracle connection helper.
   */
  public function lastInsertId($name = NULL) {
    if (!$name) {
      throw new Exception('The name of the sequence is mandatory for Oracle');
    }

    try {
      return $this->queryOracle($this->prefixTables('SELECT ' . $name . '.currval from dual'))->fetchField();
    }
    catch (\Exception $e) {
      // Ignore if CURRVAL not set. May be an insert that specified the serial
      // field.
      syslog(LOG_ERR, " currval: " . print_r(debug_backtrace(FALSE), TRUE));
    }
  }

  /**
   * {@inheritdoc}
   */
  public function generateTemporaryTableName() {
    // @todo: create a cleanup job.
    $session_id = $this->queryOracle("SELECT userenv('sessionid') FROM dual")->fetchField();
    return 'TMP_' . $session_id . '_' . $this->temporaryNameIndex++;
  }

  /**
   * {@inheritdoc}
   */
  public function quote($string, $parameter_type = \PDO::PARAM_STR) {
    return "'" . str_replace("'", "''", $string) . "'";
  }

  /**
   * {@inheritdoc}
   */
  public function prefixTables($sql) {
    $sql = parent::prefixTables($sql);
    return $this->escapeAnsi($sql);
  }

  /**
   * {@inheritdoc}
   */
  public function prepareStatement(string $query, array $options, bool $allow_row_count = FALSE): StatementInterface {
    if (!($options['allow_square_brackets'] ?? FALSE)) {
      $query = $this->quoteIdentifiers($query);
    }

    $query = $this->escapeEmptyLiterals($query);
    $query = $this->escapeAnsi($query);
    if (!$this->external) {
      $query = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($query);
    }
    $query = $this->escapeReserved($query);
    $query = $this->escapeCompatibility($query);
    $query = $this->prefixTables($query);
    $query = $this->escapeIfFunction($query);

    $options['allow_square_brackets'] = TRUE;
    return parent::prepareStatement($query, $options, $allow_row_count);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeAnsi($query) {
    if (preg_match('/^select /i', $query) &&
      !preg_match('/^select(.*)from/ims', $query)) {
      $query .= ' FROM DUAL';
    }

    $search = [
      "/([^\s\(]+) & ([^\s]+) = ([^\s\)]+)/",
      "/([^\s\(]+) & ([^\s]+) <> ([^\s\)]+)/",
      '/^RELEASE SAVEPOINT (.*)$/',
      '/([^\s\(]*) NOT REGEXP ([^\s\)]*)/',
      '/([^\s\(]*) REGEXP ([^\s\)]*)/',
    ];
    $replace = [
      "BITAND(\\1,\\2) = \\3",
      "BITAND(\\1,\\2) <> \\3",
      'SELECT \'RELEASE SAVEPOINT \\1\' FROM DUAL',
      "NOT REGEXP_LIKE(\\1, \\2)",
      "REGEXP_LIKE(\\1, \\2)",
    ];
    $query = preg_replace($search, $replace, $query);

    // Find \w quoted with " but not quoted with '. Examples:
    // COMMENT ON TABLE "match" IS 'description may contain "not_matched" text.'
    // @TODO: this match wrongly (space before after '):
    // COMMENT ON TABLE "not_match_but_should" IS ' <space'
    $query = preg_replace_callback('/(?!\B\'[^\']*)("\w+?")(?![^\']*\'\B)/',
      static function ($matches) {
        return strtoupper($matches[1]);
      },
      $query);

    return str_replace('\\"', '"', $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeEmptyLiteral($match) {
    if ($match[0] == "''") {
      return "'" . ORACLE_EMPTY_STRING_REPLACER . "'";
    }
    else {
      return $match[0];
    }
  }

  /**
   * Oracle connection helper.
   */
  private function escapeEmptyLiterals($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace_callback("/'.*?'/", array($this, 'escapeEmptyLiteral'), $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeIfFunction($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    return preg_replace("/IF\s*\((.*?),(.*?),(.*?)\)/", 'case when \1 then \2 else \3 end', $query);
  }

  /**
   * Oracle connection helper.
   */
  private function escapeReserved($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $ddl = !((boolean) preg_match('/^(select|insert|update|delete)/i', $query));

    // Uppercases all table names.
    $query = preg_replace_callback(
      '/({)(\w+)(})/',
      function ($matches) {
        return '{' . strtoupper($matches[2]) . '}';
      },
      $query);

    // Uppercases all identifiers.
    $t = explode("'", $query);
    for ($i = 0; $i < count($t); $i++) {
      if ($i % 2 == 1) {
        continue;
      }
      $t[$i] = preg_replace_callback(
        '/(")(\w+)(")/',
        function ($matches) {
          return '"' . strtoupper($matches[2]) . '"';
        },
        $t[$i]);
    }
    $query = implode("'", $t);

    // Escapes long id.
    $query = preg_replace_callback(
      '/({L#)([\d]+)(})/',
      function ($matches) {
        return '"{L#' . strtoupper($matches[2]) . '}"';
      },
      $query);

    // Escapes reserved names.
    $query = preg_replace_callback(
      '/(\:)(uid|session|file|access|mode|comment|desc|size|start|end|increment)/',
      function ($matches) {
        return $matches[1] . 'db_' . $matches[2];
      },
      $query);

    $query = preg_replace_callback(
      '/(<uid>|<session>|<file>|<access>|<mode>|<comment>|<desc>|<size>' . ($ddl ? '' : '|<date>') . ')/',
      function ($matches) {
        return '"' . strtoupper($matches[1]) . '"';
      },
      $query);

    $query = preg_replace_callback(
      '/([\(\.\s,\=])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')([,\s\=)])/',
      function ($matches) {
        return $matches[1] . '"' . strtoupper($matches[2]) . '"' . $matches[3];
      },
      $query);

    $query = preg_replace_callback(
      '/([\(\.\s,])(uid|session|file|access|mode|comment|desc|size' . ($ddl ? '' : '|date') . ')$/',
      function ($matches) {
        return $matches[1] . '"' . strtoupper($matches[2]) . '"';
      },
      $query);

    return $query;
  }

  /**
   * Oracle connection helper.
   */
  public function removeFromCachedStatements($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $iquery = md5($this->prefixTables($query));
    if (isset($this->preparedStatements[$iquery])) {
      unset($this->preparedStatements[$iquery]);
    }
  }

  /**
   * Oracle connection helper.
   */
  private function escapeCompatibility($query) {
    if (is_object($query)) {
      $query = $query->getQueryString();
    }
    $search = array(
      // Remove empty concatenations leaved by concatenate_bind_variables.
      "''||",
      "||''",

      // Translate 'IN ()' to '= NULL' they do not match anything anyway.
      "IN ()",
      "IN  ()",

      '(FALSE)',
      'POW(',
      ") AS count_alias",
      '"{URL_ALIAS}" GROUP BY path',
      "ESCAPE '\\\\'",
      'SELECT CONNECTION_ID() FROM DUAL',
      'SHOW PROCESSLIST',
      'SHOW TABLES',
    );

    $replace = array(
      "",
      "",
      "= NULL",
      "= NULL",
      "(1=0)",
      "POWER(",
      ") count_alias",
      '"{URL_ALIAS}" GROUP BY SUBSTRING_INDEX(source, \'/\', 1)',
      "ESCAPE '\\'",
      'SELECT DISTINCT sid FROM v$mystat',
      'SELECT DISTINCT stat.sid, sess.process, sess.status, sess.username, sess.schemaname, sql.sql_text FROM v$mystat stat, v$session sess, v$sql sql WHERE sql.sql_id(+) = sess.sql_id AND sess.status = \'ACTIVE\' AND sess.type = \'USER\'',
      'SELECT * FROM user_tables',
    );

    return str_replace($search, $replace, $query);
  }

  /**
   * {@inheritdoc}
   */
  public function makeSequenceName($table, $field) {
    $sequence_name = $this->schema()->oid('SEQ_' . $table . '_' . $field, FALSE, FALSE);
    return '"{' . $sequence_name . '}"';
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupArgValue($value) {
    if ($value === '') {
      return ORACLE_EMPTY_STRING_REPLACER;
    }
    return $value;
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupArgs($args) {
    if ($this->external) {
      return $args;
    }

    $ret = [];
    foreach ($args as $key => $value) {
      if (is_string($key)) {
        // Bind variables cannot have reserved names.
        $key = Connection::escapeReserved($key);
        $key = $this->getLongIdentifiersHandler()->escapeLongIdentifiers($key);
      }

      $ret[$key] = $this->cleanupArgValue($value);
    }

    return $ret;
  }

  /**
   * Add a new savepoint with a unique name.
   *
   * The main use for this method is to mimic InnoDB functionality, which
   * provides an inherent savepoint before any query in a transaction.
   *
   * @param $savepoint_name
   *   A string representing the savepoint name. By default,
   *   "mimic_implicit_commit" is used.
   *
   * @see Drupal\Core\Database\Connection::pushTransaction()
   */
  public function addSavepoint($savepoint_name = 'mimic_implicit_commit') {
    if ($this->inTransaction()) {
      $this->pushTransaction($savepoint_name);
    }
  }

  /**
   * Release a savepoint by name.
   *
   * @param $savepoint_name
   *   A string representing the savepoint name. By default,
   *   "mimic_implicit_commit" is used.
   *
   * @see Drupal\Core\Database\Connection::popTransaction()
   */
  public function releaseSavepoint($savepoint_name = 'mimic_implicit_commit') {
    if (isset($this->transactionLayers[$savepoint_name])) {
      $this->popTransaction($savepoint_name);
    }
  }

  /**
   * Rollback a savepoint by name if it exists.
   *
   * @param $savepoint_name
   *   A string representing the savepoint name. By default,
   *   "mimic_implicit_commit" is used.
   */
  public function rollbackSavepoint($savepoint_name = 'mimic_implicit_commit') {
    if (isset($this->transactionLayers[$savepoint_name])) {
      $this->rollBack($savepoint_name);
    }
  }

  /**
   * {@inheritDoc}
   */
  public function hasJson(): bool {
    return TRUE;
  }

  /**
   * Cleaned query string.
   *
   * 1) Long identifiers placeholders.
   *  May occur in queries like:
   *               select 1 as myverylongidentifier from mytable
   *  this is translated on query submission as e.g.:
   *               select 1 as L#321 from mytable
   *  so when we fetch this object (or array) we will have
   *     stdClass ( "L#321" => 1 ) or Array ( "L#321" => 1 ).
   *  but the code is expecting to access the field as myverylongidentifier,
   *  so we need to translate the "L#321" back to "myverylongidentifier".
   *
   * 2) BLOB placeholders.
   *   We can find values like B^#2354, and we have to translate those values
   *   back to their original long value so we read blob id 2354 of table blobs.
   *
   * 3) Removes the rwn column from queryRange queries.
   *
   * 4) Translate empty string replacement back to empty string.
   *
   * @return string
   *   Cleaned string to be executed.
   */
  public function cleanupFetched($f) {
    if ($this->external) {
      return $f;
    }

    if (is_array($f)) {
      foreach ($f as $key => $value) {
        // Long identifier.
        if (Connection::isLongIdentifier($key)) {
          $f[$this->getLongIdentifiersHandler()->longIdentifierKey($key)] = $this->cleanupFetched($value);
          unset($f[$key]);
        }
        else {
          $f[$key] = $this->cleanupFetched($value);
        }
      }
    }
    elseif (is_object($f)) {
      foreach ($f as $key => $value) {
        // Long identifier.
        if (Connection::isLongIdentifier($key)) {
          $f->{$this->getLongIdentifiersHandler()->longIdentifierKey($key)} = $this->cleanupFetched($value);
          unset($f->{$key});
        }
        else {
          $f->{$key} = $this->cleanupFetched($value);
        }
      }
    }
    else {
      $f = $this->cleanupFetchedValue($f);
    }

    return $f;
  }

  /**
   * Oracle connection helper.
   */
  public function cleanupFetchedValue($value) {
    if (is_string($value)) {
      if ($value == ORACLE_EMPTY_STRING_REPLACER) {
        return '';
      }
      else {
        return $value;
      }
    }
    else {
      return $value;
    }
  }

  /**
   * Oracle connection helper.
   */
  public function resetLongIdentifiers() {
    if (!$this->external) {
      $this->getLongIdentifiersHandler()->resetLongIdentifiers();
    }
  }

  /**
   * Oracle connection helper.
   */
  public static function isLongIdentifier($key) {
    return (substr(strtoupper($key), 0, strlen(ORACLE_LONG_IDENTIFIER_PREFIX)) == ORACLE_LONG_IDENTIFIER_PREFIX);
  }

  /**
   * Long identifier support.
   */
  public function getLongIdentifiersHandler() {
    static $long_identifier = NULL;

    if ($this->external) {
      return NULL;
    }

    // Initialize the long identifier handler.
    if (empty($long_identifier)) {
      $long_identifier = new LongIdentifierHandler($this);
    }
    return $long_identifier;
  }

}


/**
 * @} End of "addtogroup database".
 */

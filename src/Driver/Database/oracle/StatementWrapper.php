<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\StatementWrapper as DatabaseStatementWrapper;

/**
 * Oracle implementation of \Drupal\Core\Database\StatementWrapper
 */
class StatementWrapper extends DatabaseStatementWrapper {

  /**
   * {@inheritdoc}
   */
  public function execute($args = array(), $options = array()) {
    if (!is_array($args) || !count($args)) {
      $args = NULL;
    }
    else {
      // Cleanup parameter and values.
      $args = $this->connection->cleanupArgs($args);
    }

    return parent::execute($args, $options);
  }

  /**
   * {@inheritdoc}
   */
  public function getClientStatement() {
    return $this;
  }

    /**
   * Implements the magic __get() method.
   *
   * @todo Remove the method before Drupal 10.
   * @see https://www.drupal.org/i/3210310
   */
  public function __get($name) {
    if ($name === 'queryString') {
      @trigger_error("StatementWrapper::\$queryString should not be accessed in drupal:9.1.0 and will error in drupal:10.0.0. Access the client-level statement object via ::getClientStatement(). See https://www.drupal.org/node/3177488", E_USER_DEPRECATED);
      return $this->clientStatement->queryString;
    }

    return parent::__get($name);
  }

  /**
   * Implements the magic __call() method.
   *
   * @deprecated in drupal:9.1.0 and is removed from drupal:10.0.0. Access the
   *   client-level statement object via ::getClientStatement().
   *
   * @see https://www.drupal.org/node/3177488
   */
  public function __call($method, $arguments) {
    if (is_callable([$this->clientStatement, $method])) {
      @trigger_error("StatementWrapper::{$method} should not be called in drupal:9.1.0 and will error in drupal:10.0.0. Access the client-level statement object via ::getClientStatement(). See https://www.drupal.org/node/3177488", E_USER_DEPRECATED);
      return call_user_func_array([$this->clientStatement, $method], $arguments);
    }

    throw new \BadMethodCallException($method);
  }

  /**
   * {@inheritdoc}
   */
  public function bindParam($parameter, &$variable, $data_type = \PDO::PARAM_STR, $max_length = -1, $driver_options = null) : bool {
    if ($parameter === ':name' && $variable === NULL) {
      @trigger_error("StatementWrapper::bindParam should not be called in drupal:9.1.0 and will error in drupal:10.0.0. Access the client-level statement object via ::getClientStatement(). See https://www.drupal.org/node/3177488", E_USER_DEPRECATED);
    }

    // Cleanup parameter and values.
    $args = [$parameter => $variable];
    $args = $this->connection->cleanupArgs($args);
    $parameter = array_keys($args)[0];
    $variable = array_values($args)[0];

    if ($data_type != \PDO::PARAM_STR) {
      return $this->clientStatement->bindParam($parameter, $variable, $data_type, $max_length, $driver_options);
    }

    if ($max_length === -1) {
      $max_length = strlen((string) $variable);
    }

    $this->clientStatement->bindParam($parameter, $variable, $data_type, $max_length, $driver_options);

    // @todo Oracle driver's PDO Statement does always return FALSE.
    return TRUE;
  }

  /**
   * {@inheritdoc}
   */
  public function fetch($fetch_style = NULL, $cursor_orientation = \PDO::FETCH_ORI_NEXT, $cursor_offset = 0) {
    return $this->connection->cleanupFetched(parent::fetch($fetch_style, $cursor_orientation, $cursor_offset));
  }

  /**
   * {@inheritdoc}
   */
  public function fetchField($index = 0) {
    return $this->connection->cleanupFetched(parent::fetchField($index));
  }

  /**
   * {@inheritdoc}
   */
  public function fetchObject($class_name = 'stdClass', $ctor_args = []) {
    return $this->connection->cleanupFetched(parent::fetchObject($class_name, $ctor_args));
  }

  /**
   * {@inheritdoc}
   */
  public function fetchAssoc() {
    return $this->connection->cleanupFetched(parent::fetchAssoc());
  }

  /**
   * {@inheritdoc}
   */
  public function fetchAll($mode = NULL, $column_index = NULL, $ctor_args = NULL) {
    return $this->connection->cleanupFetched(parent::fetchAll($mode));
  }

  /**
   * {@inheritdoc}
   */
  public function fetchCol($index = 0) {
    return $this->connection->cleanupFetched(parent::fetchAll(\PDO::FETCH_COLUMN, $index));
  }

  /**
   * {@inheritdoc}
   */
  public function fetchColumn($column_number = 0) {
    return $this->connection->cleanupFetched(parent::fetchColumn($column_number));
  }

  /**
   * {@inheritdoc}
   */
  public function fetchAllKeyed($key_index = 0, $value_index = 1) {
    return $this->connection->cleanupFetched(parent::fetchAllKeyed($key_index, $value_index));
  }

  /**
   * {@inheritdoc}
   */
  public function fetchAllAssoc($key, $fetch_style = NULL) {
    return $this->connection->cleanupFetched(parent::fetchAllAssoc($key, $fetch_style));
  }

}

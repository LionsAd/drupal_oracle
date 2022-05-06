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
    return parent::execute($args, $options);
  }

  /**
   * {@inheritdoc}
   */
  public function bindParam($parameter, &$variable, $data_type = \PDO::PARAM_STR, $max_length = -1, $driver_options = null) : bool {
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

    return $this->clientStatement->bindParam($parameter, $variable, $data_type, $max_length, $driver_options);
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

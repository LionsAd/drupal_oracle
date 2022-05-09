<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\Statement as DatabaseStatement;

/**
 * Oracle implementation of \Drupal\Core\Database\Statement
 */
class Statement extends DatabaseStatement {

  /**
   * {@inheritdoc}
   */
  public function bindParam($parameter, &$variable, $data_type = \PDO::PARAM_STR, $max_length = -1, $driver_options = null) {
    // Cleanup parameter and values.
    $args = [$parameter => $variable];
    $args = $this->dbh->cleanupArgs($args);
    $parameter = array_keys($args)[0];
    $variable = array_values($args)[0];

    if ($data_type != \PDO::PARAM_STR) {
      return parent::bindParam($parameter, $variable, $data_type, $max_length, $driver_options);
    }

    if ($max_length === -1) {
      $max_length = strlen((string) $variable);
    }

    return parent::bindParam($parameter, $variable, $data_type, $max_length, $driver_options);
  }

  /**
   * {@inheritdoc}
   */
  public function rowCount() {
    $this->allowRowCount = TRUE;
    return parent::rowCount();
  }

}

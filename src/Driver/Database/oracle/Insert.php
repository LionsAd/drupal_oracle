<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Query\Insert as QueryInsert;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Insert.
 */
class Insert extends QueryInsert {

  /**
   * {@inheritdoc}
   */
  public function __construct($connection, $table, array $options = array()) {
    parent::__construct($connection, $table, $options);
    $this->queryOptions['table_name'] = $table;
  }

  /**
   * {@inheritdoc}
   */
  public function execute() {
    if (!$this->preExecute()) {
      return NULL;
    }

    $table_information = $this->connection->schema()->queryTableInformation($this->table);
    $sequence_name = NULL;
    if (!empty($table_information->sequences)) {
      $sequence_name  = $table_information->sequences[0];
    }

    $stmt = $this->connection->prepareStatement((string) $this, $this->queryOptions);

    if (!empty($this->fromQuery)) {
      foreach ($this->fromQuery->getArguments() as $key => $value) {
        $value = $this->connection->cleanupArgValue($value);
        $stmt->getClientStatement()->bindParam($key, $value);
      }
      // The SelectQuery may contain arguments, load and pass them through.
      return $this->connection->query($stmt, array(), $this->queryOptions);
    }

    $last_insert_id = 0;
    $transaction = $this->connection->startTransaction();

    try {
      if (empty($this->insertValues)) {
        $last_insert_id = $sequence_name ? $this->connection->lastInsertId($sequence_name) : 0;
      }
      else {
        foreach ($this->insertValues as &$insert_values) {
          $max_placeholder = 0;
          $blobs = [];
          $blob_count = 0;
          foreach ($this->insertFields as $idx => $field) {
            $insert_values[$idx] = $this->connection->cleanupArgValue($insert_values[$idx]);

            if (isset($table_information->blob_fields[strtoupper($field)])) {
              $blobs[$blob_count] = fopen('php://memory', 'a');
              fwrite($blobs[$blob_count], $insert_values[$idx]);
              rewind($blobs[$blob_count]);
              $stmt->getClientStatement()->bindParam(':db_insert_placeholder_' . $max_placeholder++, $blobs[$blob_count], \PDO::PARAM_LOB);

              // Pre-increment is faster in PHP than increment.
              ++$blob_count;
            }
            else {
              $stmt->getClientStatement()->bindParam(':db_insert_placeholder_' . $max_placeholder++, $insert_values[$idx]);
            }
          }
          $stmt->execute(NULL, $this->queryOptions);
        }

        $last_insert_id = $sequence_name ? $this->connection->lastInsertId($sequence_name) : 0;
      }
    }
    catch (\Exception $e) {
      // One of the INSERTs failed, rollback the whole batch.
      // Transaction already is rolled back in Connection:query().
      $transaction->rollback();

      // Rethrow the exception for the calling code.
      $this->connection->exceptionHandler()->handleExecutionException($e, $stmt, [], $this->queryOptions);
    }

    // Re-initialize the values array so that we can re-use this query.
    $this->insertValues = array();

    return $last_insert_id;
  }

  /**
   * {@inheritdoc}
   */
  public function __toString() {
    $table_information = $this->connection->schema()->queryTableInformation($this->table);

    // Default fields are always placed first for consistency.
    $insert_fields = array_merge($this->defaultFields, $this->insertFields);

    if (!empty($this->fromQuery)) {
      $cols = implode(', ', $insert_fields);
      if (!empty($cols)) {
        return "INSERT INTO {" . $this->table . '} (' . $cols . ') ' . $this->fromQuery;
      }
      else {
        return "INSERT INTO {" . $this->table . '}  ' . $this->fromQuery;
      }
    }

    $query = "INSERT INTO {" . $this->table . '} (' . implode(', ', $insert_fields) . ') VALUES ';

    $max_placeholder = 0;
    $values = array();
    $blobs = [];

    if (count($this->insertValues)) {
      $placeholders = array();
      $placeholders = array_pad($placeholders, count($this->defaultFields), 'default');
      $i = 0;
      foreach ($this->insertFields as $idx => $field) {
        if (isset($table_information->blob_fields[strtoupper($field)])) {
          $blobs[$field] = ':db_insert_placeholder_' . $i++;
          $placeholders[] = 'EMPTY_BLOB()';
        }
        else {
          $placeholders[] = ':db_insert_placeholder_' . $i++;
        }
      }
      $values = '(' . implode(', ', $placeholders) . ')';
      if (!empty($blobs)) {
        $values .= ' RETURNING ' . implode(', ', array_keys($blobs)) . ' INTO ' . implode(', ', array_values($blobs));
      }
    }
    else {
      if (count($this->defaultFields) > 0) {
        // If there are no values, then this is a default-only query.
        // We still need to handle that.
        $placeholders = array_fill(0, count($this->defaultFields), 'default');
        $values = '(' . implode(', ', $placeholders) . ')';
      }
      else {
        // Meaningless query that will not be executed.
        $values = '()';
      }
    }

    $query .= $values;

    return $query;
  }

}

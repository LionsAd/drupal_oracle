<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\Database;
use Drupal\Core\Database\Driver\pgsql\Update as QueryUpdate;
use Drupal\Core\Database\Query\SelectInterface;

use Drupal\Component\Utility\Unicode;
use Drupal\Core\Database\DatabaseExceptionWrapper;
use Drupal\Core\Database\ExceptionHandler as BaseExceptionHandler;
use Drupal\Core\Database\IntegrityConstraintViolationException;
use Drupal\Core\Database\StatementInterface;

/**
 * Oracle database exception handler class.
 */
class ExceptionHandler extends BaseExceptionHandler {

  /**
   * {@inheritdoc}
   */
  public function handleExecutionException(\Exception $exception, StatementInterface $statement, array $arguments = [], array $options = []): void {
    if (array_key_exists('throw_exception', $options)) {
      @trigger_error('Passing a \'throw_exception\' option to ' . __METHOD__ . ' is deprecated in drupal:9.2.0 and is removed in drupal:10.0.0. Always catch exceptions. See https://www.drupal.org/node/3201187', E_USER_DEPRECATED);
      if (!($options['throw_exception'])) {
        return;
      }
    }

    // @todo Implement retrying of query as old driver had?

    if ($exception instanceof \PDOException) {
      // Wrap the exception in another exception, because PHP does not allow
      // overriding Exception::getMessage(). Its message is the extra database
      // debug information.
      $code = (int) ($exception->errorInfo[1] ?? NULL);
      $message = $exception->getMessage() . ": " . $statement->getQueryString() . "; " . print_r($arguments, TRUE);

      // The old driver had 'errorInfo[1] == "1" -> errorInfo[0] = 23000',
      // which is an IntegrityConstraintViolationException().
      if (in_array($code, [1,1400])) {
        throw new IntegrityConstraintViolationException($message, $code, $exception);
      }

      throw new DatabaseExceptionWrapper($message, $code, $exception);
    }

    throw $exception;
  }

}

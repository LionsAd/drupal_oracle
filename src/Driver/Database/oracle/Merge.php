<?php

namespace Drupal\oracle\Driver\Database\oracle;

use Drupal\Core\Database\Query\Merge as QueryMerge;

/**
 * Oracle implementation of \Drupal\Core\Database\Query\Merge.
 */
class Merge extends QueryMerge {
  use OracleQueryTrait;
}

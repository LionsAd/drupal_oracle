# Driver installation.

There are two ways to install Drupal 8 Oracle Database Driver.

## 1. Composer way (the preferred one).
Step 1. Download the module.

Step 2. Add needed patches to "extra" > "patches" section in your composer.json:  
```
"extra": {
    "patches": {
        "drupal/core": {
            "Log::findCaller fails to report the correct caller function with non-core drivers": "https://www.drupal.org/files/issues/2019-05-28/2867788-53.patch",
            "non-standard precision limits at testSchemaAddFieldDefaultInitial": "https://www.drupal.org/files/issues/2020-01-28/drupal-3109651-SchemaTest_precision_limits-3.patch"
        }
    }
},
```

Step 3. Run the `composer` commands as usually:  
`composer require drupal/oracle`  
`composer install`


## 2. Manual way.

Download an archive from drupal.org project page and place the code in this
directory: `DRUPAL_ROOT/modules/contrib/oracle.

Apply all needed patches to the Drupal core and tests run:
 - https://www.drupal.org/files/issues/2019-05-28/2867788-53.patch
 - https://www.drupal.org/files/issues/2020-01-28/drupal-3109651-SchemaTest_precision_limits-3.patch

# Running tests in their own database / schema

This allows to run tests in their own database / schema: e.g. "TEST12345"."FOO" instead of "TEST12345FOO"

```bash
export ORACLE_RUN_TESTS_IN_EXTRA_DB=1
../vendor/bin/phpunit core/tests/Drupal/KernelTests/Core/Database/ --filter testDbFindTables
```

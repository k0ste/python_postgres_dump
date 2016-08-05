python_postgres_dump
===========================

Backup all PostgreSQL databases. You can exclude database or schema of database
from backup via JSON file. PostgreSQL globals backuped always.


About:
-------------

* Supported `gzip, 7z, xz` as data compressors;
* Compression level can be specified;
* All databases dumped in custom format, already compressed by tar;
* Output like date in Linux:

```
Fri Aug  5 16:13:21 2016 +0700
```

Example
-----------------

* From database example schemas 'disabled_schema' and 'slow_schema' will not be
dumped;
* Templates are disabled;

```json
{
  "database" : [
    {
      "name" : "example",
      "state" : "enabled",
      "schema" : [
        {
          "name" : "disabled_schema",
          "state" : "exclude"
        },
        {
          "name" : "slow_schema",
          "state" : "exclude"
        }
      ]
    },
    {
      "name" : "template0",
      "state" : "disabled"
    },
    {
      "name" : "template1",
      "state" : "disabled"
    }
  ]
}
```

Add to cron or use pgagent for schedule jobs:

```
python python_postgres_dump.py -H localhost -p secret_password -j /mnt/backups/pg_db.json -o /mnt/backups
```

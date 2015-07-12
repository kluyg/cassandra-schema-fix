# cassandra-schema-fix

Compare Cassandra schema and data folder content and fix the differences

Usage:

    ./cassandra-schema-fix <schema-file> <data folder>

where `<schema-file>` is a text file you can get by running:

    cqlsh -e "select keyspace_name,columnfamily_name,cf_id from system.schema_columnfamilies" > schema.txt

it has the following format:

     keyspace_name  | columnfamily_name  | cf_id
    ----------------+--------------------+--------------------------------------
          keyspace1 |      columnfamily1 | 53793600-bdf4-11e4-acab-3378dc80ad44
          keyspace2 |      columnfamily2 | 54c36940-bdf4-11e4-b22c-4b9177b5fdb8
          keyspace3 |      columnfamily3 | 56920ec0-bdf4-11e4-b89d-ef3536699cea

and `<data folder>` is usually `/var/lib/cassandra/data` or `/raid0/cassandra/data`

When you run it, it first prints the schema it parsed from the file, then it prints the schema it found
in the data folder and then it prints all the differences it found in the data folder, assuming that the
schema in the file is the correct one.

There are two types of situations it tries to resolve:

#### 1. There is something in the data folder that doesn't exist in the schema at all.

This can happen if you created a keyspace and/or a columnfamily and removed it later. It will be removed
from the schema but will remain in your data folder.

It asks you would you like to remove it from your data folder and if you confirm will do it for you.

#### 2. There is keyspace.columnfamily combination that has different cf_id from the schema.

This can happen if you create columnfamilies automatically in code, which is an anti-pattern in Cassandra
and will lead to a big trouble. This happened to me and is why I wrote this cassandra-schema-fix in a
hurry - I had ~500 columnfamilies and didn't want to deal with fixing it manually.

It asks you would you like to move SSTables from the folder with the wrong cf_id to the folder with the
correct cf_id from the schema. If you confirm, it will remove snapshots in the wrong folder and try to
move the SSTables. It's pretty dumb and if the SSTable with the same name already exists in the correct
folder, it will skip it and print a warning. I moved such files manually later.

As the last step it will run `nodetool refresh keyspace_name columnfamily_name` so that Cassandra picks
up the moved SSTables.

#### WARNING

This is a result of couple of hours of work and helped me with the problems in my cluster. If you confirm,
it tries to resolve found issues by moving / removing your data, so use it at your own risk.


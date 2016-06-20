FDWs for PostgreSQL
===============================

This Python module implements the `multicorn.ForeignDataWrapper` interface to allow you to create foreign tables in PostgreSQL 9.1+ that query to tables in Apache Hive. 

Fork from https://github.com/youngwookim/hive-fdw-for-postgresql

Add function to create foreign tables that query hive server2 and kylin

Pre-requisites
--------------

* [PostgreSQL 9.1+](http://www.postgresql.org/)
* [Python 2.7](http://python.org/)
* [Multicorn](http://multicorn.org)
* [hive-thrift-py](https://github.com/youngwookim/hive-thrift-py)
* [pyhive](https://github.com/dropbox/PyHive)
* [pykylin](https://github.com/wxiang7/pykylin)

Installation
------------

1. [Install Multicorn](http://multicorn.org/#installation)
2. [Install hive-thrift-py](https://github.com/youngwookim/hive-thrift-py)
3. Build the FDW module:

        $ cd hive-fdw-for-postgresql
        $ python setup.py sdist
        $ sudo python setup.py install

    or, with easy_install:

        $ cd hive-fdw-for-postgresql
        $ sudo easy_install .

4. In the PostgreSQL client, create an extension and foreign server:

    0)
        CREATE EXTENSION multicorn;
        
    1)
        CREATE SERVER multicorn_hive FOREIGN DATA WRAPPER multicorn
        OPTIONS (
            wrapper 'hivefdw.HiveForeignDataWrapper'
        );

    2)
        CREATE SERVER multicorn_hive_server2 FOREIGN DATA WRAPPER multicorn
        OPTIONS (
            wrapper 'hive2fdw.HiveServer2ForeignDataWrapper'
        );

    3)
        CREATE SERVER multicorn_kylin FOREIGN DATA WRAPPER multicorn
        OPTIONS (
            wrapper 'kylin.KylinForeignDataWrapper'
        );

Examples
------------

1. User can executes simple selects on a remote Hive table:

        CREATE FOREIGN TABLE hive (
            a varchar,
            b varchar,
            c varchar,
            d varchar
        ) SERVER multicorn_hive OPTIONS (
            host 'tb081',
            port '10000',
            table 'test'
        );

        SELECT * FROM hive;

2. Also user can executes selects using a Hive query:
         
        CREATE FOREIGN TABLE hive_query (
            x varchar,
            y varchar,
            z varchar
        ) SERVER multicorn_hive OPTIONS (
            host 'tb081',
            port '10000',
            query 'SELECT x,y,z from src'
        );
        
        SELECT * from hive_query;

3. Query hive server2

        CREATE FOREIGN TABLE templayer.hive_server2 (
            a varchar
        ) SERVER multicorn_hive_server2 OPTIONS (
            host 'sha2dw02',
            port '26162',
            query 'select a from default.dual'
        );
        
        select * from templayer.hive_server2

4. Query kylin

        CREATE FOREIGN TABLE templayer.kylin (
            dt date,
            cnt int
        ) SERVER multicorn_kylin OPTIONS (
            host 'sha2dw03',
            port '7070',
            user 'ADMIN',
            password 'KYLIN',
            project 'LiuLiang',
            limit '5000',
            query '
                select
                TRANSACTION_DATE,
                count(1)
                from F_T_CONTACT 
                group by F_T_CONTACT.TRANSACTION_DATE 
                limit 10
            '
        );
        select * from templayer.kylin

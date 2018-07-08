--! qt:dataset:src

set hive.strict.checks.cartesian.product= false;


CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';


FROM src

SELECT dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" FLOAT, "dkey" DOUBLE )' )

limit 1;

CREATE EXTERNAL TABLE ext_simple_derby_table1
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);

select * from ext_simple_derby_table1;

explain select bkey from ext_simple_derby_table1 where 100 < ext_simple_derby_table1.ikey;

select bkey from ext_simple_derby_table1 where 100 < ext_simple_derby_table1.ikey;



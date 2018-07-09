--! qt:dataset:src

set hive.strict.checks.cartesian.product= false;


CREATE TABLE SIMPLE_HIVE_TABLE1 (ikey INT, bkey BIGINT, fkey FLOAT, dkey DOUBLE );

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';


FROM src

SELECT dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" FLOAT, "dkey" DOUBLE )' )

limit 1;

FROM src

SELECT dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" FLOAT, "dkey" DOUBLE )' )

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


CREATE EXTERNAL TABLE ext_simple_derby_table2
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
                "hive.sql.table" = "SIMPLE_DERBY_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);

select * from ext_simple_derby_table1;

explain select bkey from ext_simple_derby_table1 where 100 < ext_simple_derby_table1.ikey;

select bkey from ext_simple_derby_table1 where 100 < ext_simple_derby_table1.ikey;

--Test projection
select count(*) from ext_simple_derby_table1;

select count (distinct bkey) from ext_simple_derby_table1;

select dkey,fkey,bkey,ikey from ext_simple_derby_table1;

select abs(dkey),abs(ikey),abs(fkey),abs(bkey) from ext_simple_derby_table1;

--Test filter

SELECT distinct dkey from ext_simple_derby_table1 where ikey = '100';
SELECT count(*) FROM (select * from ext_simple_derby_table1) v WHERE ikey = 100;
SELECT count(*) from ext_simple_derby_table1 having count(*) > 0;
--select sum(bkey) from ext_simple_derby_table1 where ikey = 2450894 OR ikey = 2450911;
select sum(8),8 from ext_simple_derby_table1 where ikey = 1 group by 2;


--Test join
explain select ext_simple_derby_table1.fkey, ext_simple_derby_table2.dkey from ext_simple_derby_table1 join ext_simple_derby_table2 on
(ext_simple_derby_table1.ikey = ext_simple_derby_table2.ikey);

select ext_simple_derby_table1.fkey, ext_simple_derby_table2.dkey from ext_simple_derby_table1 join ext_simple_derby_table2 on
(ext_simple_derby_table1.ikey = ext_simple_derby_table2.ikey);







--The following does not work due to invalid generated derby syntax:
--SELECT "dkey", COUNT("bkey") AS "$f1" FROM "SIMPLE_DERBY_TABLE1" GROUP BY "dkey" OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY {LIMIT 1}

--SELECT  dkey,count(bkey) from ext_simple_derby_table1 group by dkey limit 10;





--Fails parse.CalcitePlanner: CBO failed, skipping CBO.
--select sum(fkey) from ext_simple_derby_table1 where bkey in (10, 100);
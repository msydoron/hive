/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.dao;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An iterator that allows iterating through a SQL resultset. Includes methods to clear up resources.
 */
public class JdbcRecordIterator implements Iterator<Map<String, Object>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordIterator.class);

  private Connection conn;
  private PreparedStatement ps;
  private ResultSet rs;


  public JdbcRecordIterator(Connection conn, PreparedStatement ps, ResultSet rs) {
    this.conn = conn;
    this.ps = ps;
    this.rs = rs;
  }


  @Override
  public boolean hasNext() {
    try {
      return rs.next();
    }
    catch (Exception se) {
      LOGGER.warn("hasNext() threw exception", se);
      return false;
    }
  }


  @Override
  public Map<String, Object> next() {
    try {
      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      Map<String, Object> record = new HashMap<String, Object>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        String key = metadata.getColumnName(i + 1);
        Object value;
        // This is not a complete list, barely make information schema work
        switch (metadata.getColumnTypeName(i+1).toLowerCase()) {
          case "int":
          case "integer":
          case "smallint":
          case "tinyint":
            value = rs.getInt(i + 1);
            break;
          case "bigint":
            value = rs.getLong(i + 1);
            break;
          case "float":
            value = rs.getFloat(i + 1);
            break;
          case "double":
            value = rs.getDouble(i + 1);
            break;
          case "bigdecimal":
            value = HiveDecimal.create(rs.getBigDecimal(i + 1));
            break;
          case "boolean":
            value = rs.getBoolean(i + 1);
            break;
          case "string":
          case "char":
          case "varchar":
          case "long varchar":
            value = rs.getString(i + 1);
            break;
          case "datetime":
          case "time":
            value = rs.getDate(i + 1);
            break;
          case "timestamp":
            java.sql.Timestamp dbTs = rs.getTimestamp(i + 1);
            if(dbTs != null) {
              org.apache.hadoop.hive.common.type.Timestamp hiveTs = new org.apache.hadoop.hive.common.type.Timestamp();
              hiveTs.setTimeInMillis(dbTs.getTime(), dbTs.getNanos());
              assert hiveTs.toEpochMilli() == dbTs.getTime();
              value = hiveTs;
            } else {
              value = null;
            }
            break;
          default:
            value = rs.getObject(i + 1);
            break;
        }

        record.put(key, value);
      }

      return record;
    }
    catch (Exception e) {
      LOGGER.warn("next() threw exception", e);
      return null;
    }
  }


  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not supported");
  }


  /**
   * Release all DB resources
   */
  public void close() {
    try {
      rs.close();
      ps.close();
      conn.close();
    }
    catch (Exception e) {
      LOGGER.warn("Caught exception while trying to close database objects", e);
    }
  }

}

package com.sogou.beaver.core.engine;

import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class PrestoEngine implements SQLEngine {
  private final Logger LOG = LoggerFactory.getLogger(PrestoEngine.class);

  private final JDBCConnectionPool pool;
  private final String dataFile;

  public PrestoEngine(JDBCConnectionPool pool, long jobId) {
    this.pool = pool;
    this.dataFile = jobId + ".data";
  }

  @Override
  public boolean execute(String sql) {
    FileOutputCollector collector;
    try {
      collector = new FileOutputCollector(dataFile);
    } catch (IOException e) {
      LOG.error("Failed to init collector", e);
      return false;
    }

    Connection conn;
    try {
      conn = pool.getConnection();
    } catch (ConnectionPoolException e) {
      LOG.error("Failed to get connection", e);
      return false;
    }

    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnNum = rsmd.getColumnCount();
        List<String> columnNames = getColumnNames(rsmd, columnNum);
        collector.collect(columnNames);
        while (rs.next()) {
          List<String> values = getValues(rsmd, rs, columnNum);
          collector.collect(values);
        }
      }
      return true;
    } catch (SQLException e) {
      LOG.error("Failed to execute sql: " + sql, e);
    } catch (IOException e) {
      LOG.error("Failed to collect result", e);
    } finally {
      try {
        pool.releaseConnection(conn);
      } catch (ConnectionPoolException e) {
        LOG.error("Failed to release conn");
      }
      try {
        collector.close();
      } catch (IOException e) {
        LOG.error("Failed to close collector", e);
      }
    }

    return false;
  }

  private List<String> getColumnNames(ResultSetMetaData rsmd, int columnNum) throws SQLException {
    List<String> columnNames = new ArrayList<>();
    for (int i = 1; i <= columnNum; i++) {
      columnNames.add(rsmd.getColumnName(i));
    }
    return columnNames;
  }

  private List<String> getValues(ResultSetMetaData rsmd, ResultSet rs, int columnNum)
      throws SQLException {
    List<String> values = new ArrayList<>();
    for (int i = 1; i <= columnNum; i++) {
      String value;
      String columnName = rsmd.getColumnName(i);
      switch (rsmd.getColumnType(i)) {
        case java.sql.Types.ARRAY:
          value = String.valueOf(rs.getArray(columnName));
          break;
        case java.sql.Types.BIGINT:
          value = String.valueOf(rs.getInt(columnName));
          break;
        case java.sql.Types.BOOLEAN:
          value = String.valueOf(rs.getBoolean(columnName));
          break;
        case java.sql.Types.BLOB:
          value = String.valueOf(rs.getBlob(columnName));
          break;
        case java.sql.Types.DOUBLE:
          value = String.valueOf(rs.getDouble(columnName));
          break;
        case java.sql.Types.FLOAT:
          value = String.valueOf(rs.getFloat(columnName));
          break;
        case java.sql.Types.INTEGER:
        case java.sql.Types.TINYINT:
        case java.sql.Types.SMALLINT:
          value = String.valueOf(rs.getInt(columnName));
          break;
        case java.sql.Types.NVARCHAR:
          value = String.valueOf(rs.getNString(columnName));
          break;
        case java.sql.Types.VARCHAR:
          value = String.valueOf(rs.getString(columnName));
          break;
        case java.sql.Types.DATE:
          value = String.valueOf(rs.getDate(columnName));
          break;
        case java.sql.Types.TIMESTAMP:
          value = String.valueOf(rs.getTimestamp(columnName));
          break;
        default:
          value = String.valueOf(rs.getObject(columnName));
          break;
      }
      values.add(i - 1, value);
    }
    return values;
  }
}

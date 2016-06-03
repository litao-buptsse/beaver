package com.sogou.beaver.core.engine;

import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class PrestoEngine implements SQLEngine {
  private final Logger LOG = LoggerFactory.getLogger(PrestoEngine.class);

  private final JDBCConnectionPool pool;

  public PrestoEngine(JDBCConnectionPool pool) {
    this.pool = pool;
  }

  @Override
  public boolean execute(String sql) {
    Connection conn = null;
    try {
      conn = pool.getConnection();
      try (Statement stmt = conn.createStatement()) {
        try (ResultSet rs = stmt.executeQuery(sql)) {
          ResultSetMetaData rsmd = rs.getMetaData();
          int columnNum = rsmd.getColumnCount();
          List<String> columnNames = new ArrayList<>();
          for (int i = 1; i <= columnNum; i++) {
            columnNames.add(rsmd.getColumnName(i));
          }

          List<List<String>> result = new ArrayList<>();
          result.add(columnNames);

          while (rs.next()) {
            List<String> values = new ArrayList<>();
            for (int i = 1; i <= columnNum; i++) {
              String columnName = rsmd.getColumnName(i);
              switch (rsmd.getColumnType(i)) {
                case java.sql.Types.ARRAY:
                  values.add(i - 1, String.valueOf(rs.getArray(columnName)));
                  break;
                case java.sql.Types.BIGINT:
                  values.add(i - 1, String.valueOf(rs.getInt(columnName)));
                  break;
                case java.sql.Types.BOOLEAN:
                  values.add(i - 1, String.valueOf(rs.getBoolean(columnName)));
                  break;
                case java.sql.Types.BLOB:
                  values.add(i - 1, String.valueOf(rs.getBlob(columnName)));
                  break;
                case java.sql.Types.DOUBLE:
                  values.add(i - 1, String.valueOf(rs.getDouble(columnName)));
                  break;
                case java.sql.Types.FLOAT:
                  values.add(i - 1, String.valueOf(rs.getFloat(columnName)));
                  break;
                case java.sql.Types.INTEGER:
                case java.sql.Types.TINYINT:
                case java.sql.Types.SMALLINT:
                  values.add(i - 1, String.valueOf(rs.getInt(columnName)));
                  break;
                case java.sql.Types.NVARCHAR:
                  values.add(i - 1, String.valueOf(rs.getNString(columnName)));
                  break;
                case java.sql.Types.VARCHAR:
                  values.add(i - 1, String.valueOf(rs.getString(columnName)));
                  break;
                case java.sql.Types.DATE:
                  values.add(i - 1, String.valueOf(rs.getDate(columnName)));
                  break;
                case java.sql.Types.TIMESTAMP:
                  values.add(i - 1, String.valueOf(rs.getTimestamp(columnName)));
                  break;
                default:
                  values.add(i - 1, String.valueOf(rs.getObject(columnName)));
                  break;
              }
            }

            result.add(values);
          }

          result.stream()
              .map(list -> list.stream().collect(Collectors.joining(", ")))
              .forEach(line -> System.out.println(line));
          return true;
        }
      }
    } catch (ConnectionPoolException e) {
      LOG.error("Failed to get connection", e);
    } catch (SQLException e) {
      LOG.error("Failed to execute sql: " + sql, e);
    } finally {
      try {
        pool.releaseConnection(conn);
      } catch (ConnectionPoolException e) {
        LOG.error("Failed to release connection", e);
      }
    }
    return false;
  }
}

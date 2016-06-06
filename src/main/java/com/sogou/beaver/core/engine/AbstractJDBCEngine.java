package com.sogou.beaver.core.engine;

import com.sogou.beaver.core.collector.OutputCollector;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/6.
 */
public abstract class AbstractJDBCEngine extends AbstractSQLEngine {
  private final Logger LOG = LoggerFactory.getLogger(AbstractJDBCEngine.class);

  @Override
  public boolean doExecute(String sql, OutputCollector collector) throws SQLException {
    JDBCConnectionPool pool = getJDBCConnectionPool();
    Connection conn = null;
    try {
      conn = pool.getConnection();
      return doExecute(sql, conn, collector);
    } catch (ConnectionPoolException e) {
      throw new SQLException("Failed to get connection", e);
    } finally {
      try {
        pool.releaseConnection(conn);
      } catch (ConnectionPoolException e) {
        LOG.error("Failed to release conn");
      }
    }
  }

  public abstract boolean doExecute(String sql, Connection conn, OutputCollector collector)
      throws SQLException;

  public abstract JDBCConnectionPool getJDBCConnectionPool();

  protected List<String> getColumnNames(ResultSetMetaData rsmd, int columnNum) throws SQLException {
    List<String> columnNames = new ArrayList<>();
    for (int i = 1; i <= columnNum; i++) {
      columnNames.add(rsmd.getColumnName(i));
    }
    return columnNames;
  }

  protected List<String> getColumnValues(ResultSetMetaData rsmd, ResultSet rs, int columnNum)
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

package com.sogou.beaver.core.engine;

import com.sogou.beaver.core.collector.RelationOutputCollector;
import com.sogou.beaver.core.meta.ColumnMeta;
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
  public boolean doExecute(String sql, RelationOutputCollector collector)
      throws EngineExecutionException {
    JDBCConnectionPool pool = getJDBCConnectionPool();
    Connection conn = null;
    try {
      conn = pool.getConnection();
      return doExecute(sql, conn, collector);
    } catch (ConnectionPoolException e) {
      throw new EngineExecutionException("Failed to get connection", e);
    } finally {
      try {
        pool.releaseConnection(conn);
      } catch (ConnectionPoolException e) {
        LOG.error("Failed to release conn");
      }
    }
  }

  public abstract boolean doExecute(String sql, Connection conn, RelationOutputCollector collector)
      throws EngineExecutionException;

  public abstract JDBCConnectionPool getJDBCConnectionPool();

  protected List<ColumnMeta> getColumnMetas(ResultSetMetaData rsmd) throws SQLException {
    List<ColumnMeta> columnMetas = new ArrayList<>();
    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
      columnMetas.add(new ColumnMeta(
          rsmd.getColumnName(i), rsmd.getColumnType(i), rsmd.getColumnTypeName(i)));
    }
    return columnMetas;
  }

  protected List<String> getColumnValues(List<ColumnMeta> columnMetas, ResultSet rs)
      throws SQLException {
    List<String> values = new ArrayList<>();
    for (ColumnMeta meta : columnMetas) {
      String value;
      String columnName = meta.getColumnName();
      switch (meta.getColumnType()) {
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
      values.add(value);
    }
    return values;
  }
}

package com.sogou.beaver.core.engine;

import com.sogou.beaver.core.collector.FileOutputCollector;
import com.sogou.beaver.core.collector.RelationOutputCollector;
import com.sogou.beaver.core.meta.ColumnMeta;
import com.sogou.beaver.db.JDBCConnectionPool;

import java.io.IOException;
import java.sql.*;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class PrestoEngine extends AbstractJDBCEngine {
  private final JDBCConnectionPool pool;
  private final String dataFile;

  public PrestoEngine(JDBCConnectionPool pool, long jobId) {
    this.pool = pool;
    this.dataFile = jobId + ".data";
  }

  @Override
  public boolean doExecute(String sql, Connection conn, RelationOutputCollector collector)
      throws EngineExecutionException {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData rsmd = rs.getMetaData();
        List<ColumnMeta> columnMetas = getColumnMetas(rsmd);
        collector.initColumnMetas(columnMetas);
        while (rs.next()) {
          List<String> values = getColumnValues(columnMetas, rs);
          collector.collect(values);
        }
      }
      return true;
    } catch (SQLException e) {
      throw new EngineExecutionException("Failed to execute sql: " + sql, e);
    } catch (IOException e) {
      throw new EngineExecutionException("Failed to collect result", e);
    }
  }

  @Override
  public JDBCConnectionPool getJDBCConnectionPool() {
    return pool;
  }

  @Override
  public RelationOutputCollector getRelationOutputCollector() throws IOException {
    return new FileOutputCollector(dataFile);
  }
}

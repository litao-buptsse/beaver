package com.sogou.beaver.core.engine;

import com.sogou.beaver.core.collector.FileOutputCollector;
import com.sogou.beaver.core.collector.OutputCollector;
import com.sogou.beaver.db.JDBCConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class PrestoEngine extends AbstractJDBCEngine {
  private final Logger LOG = LoggerFactory.getLogger(PrestoEngine.class);

  private final JDBCConnectionPool pool;
  private final String dataFile;

  public PrestoEngine(JDBCConnectionPool pool, long jobId) {
    this.pool = pool;
    this.dataFile = jobId + ".data";
  }

  @Override
  public boolean doExecute(String sql, Connection conn, OutputCollector collector)
      throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnNum = rsmd.getColumnCount();
        List<String> columnNames = getColumnNames(rsmd, columnNum);
        collector.collect(columnNames);
        while (rs.next()) {
          List<String> values = getColumnValues(rsmd, rs, columnNum);
          collector.collect(values);
        }
      }
      return true;
    } catch (IOException e) {
      throw new SQLException("Failed to collect result", e);
    }
  }

  @Override
  public JDBCConnectionPool getJDBCConnectionPool() {
    return pool;
  }

  @Override
  public OutputCollector getOutputCollector() throws IOException {
    return new FileOutputCollector(dataFile);
  }
}

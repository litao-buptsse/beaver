package com.sogou.beaver.core.collector;

import com.sogou.beaver.core.meta.ColumnMeta;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/6.
 */
public class MySQLOutputCollector implements RelationOutputCollector {
  private final static String DRIVER = "com.mysql.jdbc.Driver";
  private final Connection conn;
  private final String tableName;

  public MySQLOutputCollector(String url, String tableName) throws IOException {
    try {
      Class.forName(DRIVER);
      conn = DriverManager.getConnection(url);
      this.tableName = tableName;
    } catch (ClassNotFoundException | SQLException e) {
      throw new IOException("Failed to create connection", e);
    }
  }

  @Override
  public void initColumnMetas(List<ColumnMeta> columnMetadatas) {
    // create table with column meta
  }

  @Override
  public void collect(List<String> values) throws IOException {
    // insert into mysql
  }

  @Override
  public void close() throws IOException {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        throw new IOException("Failed to close connection", e);
      }
    }
  }
}

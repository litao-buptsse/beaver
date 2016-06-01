package com.sogou.beaver.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by Tao Li on 4/11/16.
 */
public class JDBCConnectionPool extends ConnectionPool<Connection> {
  private String url;
  private Properties info;

  public JDBCConnectionPool(String driver, String url) throws ConnectionPoolException {
    this(driver, url, null);
  }

  public JDBCConnectionPool(String driver, String url, Properties info) throws ConnectionPoolException {
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new ConnectionPoolException(e);
    }
    this.url = url;
    this.info = info;
  }

  @Override
  protected Connection createConnection() throws IOException {
    try {
      if (info == null) {
        return DriverManager.getConnection(url);
      } else {
        return DriverManager.getConnection(url, info);
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void closeConnection(Connection conn) throws IOException {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
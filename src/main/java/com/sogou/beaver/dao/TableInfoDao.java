package com.sogou.beaver.dao;

import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
public class TableInfoDao {
  private final Logger LOG = LoggerFactory.getLogger(TableInfoDao.class);

  private final static String TABLE_NAME = "dw_tables";
  private final JDBCConnectionPool pool;

  public TableInfoDao(JDBCConnectionPool pool) {
    this.pool = pool;
  }

  public List<TableInfo> getAllTableInfos() throws ConnectionPoolException, SQLException {
    String sql = String.format("SELECT * FROM %s", TABLE_NAME);
    Connection conn = pool.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<TableInfo> tableInfos = new ArrayList<>();
          while (rs.next()) {
            tableInfos.add(new TableInfo(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("frequency")
            ));
          }
          return tableInfos;
        }
      }
    } finally {
      pool.releaseConnection(conn);
    }
  }
}

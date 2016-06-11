package com.sogou.beaver.dao;

import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.FieldInfo;
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
public class FieldInfoDao {
  private final Logger LOG = LoggerFactory.getLogger(FieldInfoDao.class);

  private final static String TABLE_NAME = "dw_fields";
  private final JDBCConnectionPool pool;

  public FieldInfoDao(JDBCConnectionPool pool) {
    this.pool = pool;
  }

  public List<FieldInfo> getFieldInfosByTableId(long tableId)
      throws ConnectionPoolException, SQLException {
    String sql = String.format("SELECT * FROM %s WHERE tableId=%s", TABLE_NAME, tableId);
    Connection conn = pool.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<FieldInfo> fieldInfos = new ArrayList<>();
          while (rs.next()) {
            fieldInfos.add(new FieldInfo(
                rs.getLong("id"),
                rs.getLong("tableId"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("type")
            ));
          }
          return fieldInfos;
        }
      }
    } finally {
      pool.releaseConnection(conn);
    }
  }
}

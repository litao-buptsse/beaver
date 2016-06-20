package com.sogou.beaver.dao;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.FieldInfo;

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
  private final static String TABLE_NAME = "dw_fields";

  public List<FieldInfo> getFieldInfosByTableId(long tableId)
      throws ConnectionPoolException, SQLException {
    String sql = String.format("SELECT * FROM %s WHERE tableId=%s", TABLE_NAME, tableId);
    Connection conn = Config.POOL.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<FieldInfo> fieldInfos = new ArrayList<>();
          while (rs.next()) {
            fieldInfos.add(new FieldInfo(
                rs.getLong("id"),
                rs.getLong("tableId"),
                rs.getString("name"),
                rs.getString("comment"),
                rs.getString("description"),
                rs.getString("dataType"),
                rs.getString("fieldType"),
                rs.getBoolean("isEnum")
            ));
          }
          return fieldInfos;
        }
      }
    } finally {
      Config.POOL.releaseConnection(conn);
    }
  }
}

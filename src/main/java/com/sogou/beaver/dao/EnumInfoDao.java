package com.sogou.beaver.dao;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.EnumInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
public class EnumInfoDao {
  private final static String TABLE_NAME = "dw_enums";

  public List<EnumInfo> getEnumInfosByFieldId(long fieldId)
      throws ConnectionPoolException, SQLException {
    String sql = String.format("SELECT * FROM %s WHERE online=1 AND fieldId=%s", TABLE_NAME, fieldId);
    Connection conn = Config.POOL.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<EnumInfo> enumInfos = new ArrayList<>();
          while (rs.next()) {
            enumInfos.add(new EnumInfo(
                rs.getLong("id"),
                rs.getLong("fieldId"),
                rs.getString("value"),
                rs.getString("description")
            ));
          }
          return enumInfos;
        }
      }
    } finally {
      Config.POOL.releaseConnection(conn);
    }
  }
}

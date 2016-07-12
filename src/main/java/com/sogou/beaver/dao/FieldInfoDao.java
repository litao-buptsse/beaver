package com.sogou.beaver.dao;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.FieldInfo;
import com.sogou.beaver.model.TableInfo;

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

  public List<FieldInfo> getFieldInfosByViewId(long viewId)
      throws ConnectionPoolException, SQLException {
    List<FieldInfo> fieldInfos = new ArrayList<>();

    TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoById(viewId);
    if (tableInfo == null) {
      return fieldInfos;
    }

    String explodeField = tableInfo.getExplodeField();

    String sql = String.format("SELECT * FROM %s WHERE online=1 AND tableId=%s " +
        "ORDER BY description", TABLE_NAME, tableInfo.getTableId());
    Connection conn = Config.POOL.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            String name = rs.getString("name");
            boolean isArrayField = name.contains(".");
            String arrayField = name.split("\\.")[0];

            if (isArrayField && (explodeField == null || !explodeField.equals(arrayField))) {
              continue;
            }

            fieldInfos.add(new FieldInfo(
                rs.getLong("id"),
                rs.getLong("tableId"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("comment"),
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

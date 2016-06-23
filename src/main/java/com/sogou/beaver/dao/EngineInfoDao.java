package com.sogou.beaver.dao;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.EngineInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
public class EngineInfoDao {
  private final static String TABLE_NAME = "engines";

  public List<EngineInfo> getAllEngineInfos() throws ConnectionPoolException, SQLException {
    String sql = String.format("SELECT * FROM %s WHERE online=1", TABLE_NAME);
    Connection conn = Config.POOL.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<EngineInfo> engineInfos = new ArrayList<>();
          while (rs.next()) {
            engineInfos.add(new EngineInfo(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getString("description")
            ));
          }
          return engineInfos;
        }
      }
    } finally {
      Config.POOL.releaseConnection(conn);
    }
  }
}

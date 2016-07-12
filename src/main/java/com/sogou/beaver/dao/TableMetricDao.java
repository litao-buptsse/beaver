package com.sogou.beaver.dao;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.TableMetric;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
public class TableMetricDao {
  private final static String TABLE_NAME = "dw_metrics";

  public List<TableMetric> getTableMetricsByViewId(long viewId)
      throws ConnectionPoolException, SQLException {
    List<TableMetric> tableMetrics = new ArrayList<>();

    String sql = String.format("SELECT * FROM %s WHERE online=1 AND tableId=%s "
        + "ORDER BY description", TABLE_NAME, viewId);
    Connection conn = Config.POOL.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            tableMetrics.add(new TableMetric(
                rs.getLong("id"),
                rs.getLong("tableId"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("expression")
            ));
          }
          return tableMetrics;
        }
      }
    } finally {
      Config.POOL.releaseConnection(conn);
    }
  }
}

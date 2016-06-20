package com.sogou.beaver.dao;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.MethodInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
public class MethodInfoDao {
  private final static String METHOD_METRIC_TABLE_NAME = "method_metrics";
  private final static String METHOD_FILTER_TABLE_NAME = "method_filters";

  public MethodInfo getMethodInfo() throws ConnectionPoolException, SQLException {
    Connection conn = Config.POOL.getConnection();
    try {
      List<MethodInfo.MetricMethod> metricMethods = new ArrayList<>();
      String sql = String.format("SELECT * FROM %s", METHOD_METRIC_TABLE_NAME);
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            metricMethods.add(new MethodInfo.MetricMethod(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("types")
            ));
          }
        }
      }
      List<MethodInfo.FilterMethod> filterMethods = new ArrayList<>();
      sql = String.format("SELECT * FROM %s", METHOD_FILTER_TABLE_NAME);
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            filterMethods.add(new MethodInfo.FilterMethod(
                rs.getLong("id"),
                rs.getString("name"),
                rs.getString("description"),
                rs.getString("types")
            ));
          }
        }
      }
      return new MethodInfo(metricMethods, filterMethods);
    } finally {
      Config.POOL.releaseConnection(conn);
    }
  }
}

package com.sogou.beaver.dao;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
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
public class TableInfoDao {
  private final static String TABLE_TABLE_NAME = "dw_tables";
  private final static String VIEW_TABLE_NAME = "dw_views";

  private List<TableInfo> getTableInfos(String filterClause)
      throws ConnectionPoolException, SQLException {
    String sql = String.format(
        "SELECT * FROM %s v JOIN %s t ON v.tableId=t.id AND v.online=1 AND t.online=1 %s",
        VIEW_TABLE_NAME, TABLE_TABLE_NAME, filterClause);
    Connection conn = Config.POOL.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<TableInfo> tableInfos = new ArrayList<>();
          while (rs.next()) {
            tableInfos.add(new TableInfo(
                rs.getLong("v.id"),
                rs.getLong("v.tableId"),
                rs.getString("t.database"),
                rs.getString("t.tableName"),
                rs.getString("v.description"),
                rs.getString("t.frequency"),
                rs.getString("t.fileFormat"),
                rs.getString("v.explodeField"),
                rs.getString("v.preFilterSQL")
            ));
          }
          return tableInfos;
        }
      }
    } finally {
      Config.POOL.releaseConnection(conn);
    }
  }

  private TableInfo getTableInfo(String filterClause) throws ConnectionPoolException, SQLException {
    List<TableInfo> tableInfos = getTableInfos(filterClause);
    return tableInfos.size() == 0 ? null : tableInfos.get(0);
  }

  public List<TableInfo> getAllTableInfos() throws ConnectionPoolException, SQLException {
    return getTableInfos("");
  }

  public TableInfo getTableInfoById(long id) throws ConnectionPoolException, SQLException {
    return getTableInfo(String.format("AND v.id='%s'", id));
  }

  public TableInfo getTableInfoByName(String name) throws ConnectionPoolException, SQLException {
    String[] arr = name.split("\\.");
    if (arr.length != 2) {
      return null;
    }

    String database = arr[0];
    String[] arr2 = arr[1].split(":");
    String tableName = arr2[0];

    String sql = String.format("AND t.`database`='%s' AND t.tableName='%s'",
        database, tableName);

    if (arr2.length == 1) {
      sql = sql + " AND (v.explodeField is null OR v.explodeField='')";
    } else {
      sql = sql + " AND v.explodeField='" + arr2[1] + "'";
    }

    return getTableInfo(sql);
  }
}

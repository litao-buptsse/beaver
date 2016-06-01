package com.sogou.beaver.dao;

import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.util.CommonUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class JobDao {
  private final static String TABLE_NAME = "jobs";
  private final JDBCConnectionPool pool;

  public JobDao(JDBCConnectionPool pool) {
    this.pool = pool;
  }

  public boolean createJob(Job job) throws ConnectionPoolException, SQLException {
    String sql = String.format(
        "INSERT INTO %s (userId, state, startTime, queryTerm) VALUES('%s', '%s', '%s', '%s')",
        TABLE_NAME, job.getUserId(), "WAIT", CommonUtils.now(), job.getQueryTerm());
    Connection conn = pool.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        return stmt.execute();
      }
    } finally {
      pool.releaseConnection(conn);
    }
  }
}

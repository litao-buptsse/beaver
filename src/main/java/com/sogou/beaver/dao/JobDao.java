package com.sogou.beaver.dao;

import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.util.CommonUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
        "INSERT INTO %s (userId, state, startTime, queryTerm) VALUES(?, 'WAIT', ?, ?)", TABLE_NAME);
    Connection conn = pool.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, job.getUserId());
        stmt.setString(2, CommonUtils.now());
        stmt.setString(3, job.getQueryTerm());
        return stmt.execute();
      }
    } finally {
      pool.releaseConnection(conn);
    }
  }

  public List<Job> getJobs(String whereClause) throws ConnectionPoolException, SQLException {
    String sql = String.format("SELECT * FROM %s %s", TABLE_NAME, whereClause);
    Connection conn = pool.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        try (ResultSet rs = stmt.executeQuery()) {
          List<Job> jobs = new ArrayList<>();
          while (rs.next()) {
            jobs.add(new Job(
                rs.getLong("id"),
                rs.getString("userId"),
                rs.getString("state"),
                rs.getString("startTime"),
                rs.getString("endTime"),
                rs.getString("queryTerm"),
                rs.getString("executionPlan"),
                rs.getString("host"),
                rs.getString("reportURL")
            ));
          }
          return jobs;
        }
      }
    } finally {
      pool.releaseConnection(conn);
    }
  }

  public List<Job> getJobsByState(String state, long limit) throws ConnectionPoolException, SQLException {
    String whereClause = String.format("WHERE state='%s' ORDER BY ID ASC LIMIT %s", state, limit);
    return getJobs(whereClause);
  }

  public List<Job> getJobsByUserId(String userId, long page, long size)
      throws ConnectionPoolException, SQLException {
    String whereClause = String.format(
        "WHERE userId='%s' ORDER BY id DESC LIMIT %s, %s", userId, (page - 1) * size, size);
    return getJobs(whereClause);
  }
}

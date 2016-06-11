package com.sogou.beaver.dao;

import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class JobDao {
  private final Logger LOG = LoggerFactory.getLogger(JobDao.class);

  private final static String TABLE_NAME = "jobs";
  private final JDBCConnectionPool pool;

  public JobDao(JDBCConnectionPool pool) {
    this.pool = pool;
  }

  private boolean execute(String sql) throws ConnectionPoolException, SQLException {
    LOG.debug("sql: " + sql);
    Connection conn = pool.getConnection();
    try {
      try (PreparedStatement stmt = conn.prepareStatement(sql)) {
        return stmt.execute();
      }
    } finally {
      pool.releaseConnection(conn);
    }
  }

  public void createJob(Job job) throws ConnectionPoolException, SQLException {
    execute(String.format(
        "INSERT INTO %s (userId, state, startTime, queryPlan, executionPlan) " +
            "VALUES(%s, 'WAIT', %s, %s, %s)", TABLE_NAME,
        CommonUtils.formatSQLValue(job.getUserId()),
        CommonUtils.formatSQLValue(CommonUtils.now()),
        CommonUtils.formatSQLValue(job.getQueryPlan()),
        CommonUtils.formatSQLValue(job.getExecutionPlan())));
  }

  private void updateJob(Job job, String whereClause) throws ConnectionPoolException, SQLException {
    execute(String.format(
        "UPDATE %s SET userId=%s, state=%s, startTime=%s, endTime=%s, " +
            "queryPlan=%s, executionPlan=%s, host=%s %s", TABLE_NAME,
        CommonUtils.formatSQLValue(job.getUserId()),
        CommonUtils.formatSQLValue(job.getState()),
        CommonUtils.formatSQLValue(job.getStartTime()),
        CommonUtils.formatSQLValue(job.getEndTime()),
        CommonUtils.formatSQLValue(job.getQueryPlan()),
        CommonUtils.formatSQLValue(job.getExecutionPlan()),
        CommonUtils.formatSQLValue(job.getHost()),
        whereClause));
  }

  public void updateJobById(Job job, long id) throws ConnectionPoolException, SQLException {
    updateJob(job, String.format("WHERE id=%s", id));
  }

  public void updateJobByIdAndStateAndHost(Job job, long id, String state, String host)
      throws ConnectionPoolException, SQLException {
    updateJob(job, String.format("WHERE id=%s AND state='%s' AND host='%s'", id, state, host));
  }

  public void updateJobsStateAndHostByIds(String state, String host, long[] ids)
      throws ConnectionPoolException, SQLException {
    execute(String.format(
        "UPDATE %s SET state=%s, host=%s WHERE id in (%s)", TABLE_NAME,
        CommonUtils.formatSQLValue(state),
        CommonUtils.formatSQLValue(host),
        LongStream.of(ids).mapToObj(id -> String.valueOf(id)).collect(Collectors.joining(", "))));
  }

  private List<Job> getJobs(String whereClause) throws ConnectionPoolException, SQLException {
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
                rs.getString("queryPlan"),
                rs.getString("executionPlan"),
                rs.getString("host")
            ));
          }
          return jobs;
        }
      }
    } finally {
      pool.releaseConnection(conn);
    }
  }

  private Job getJob(String whereClause) throws ConnectionPoolException, SQLException {
    List<Job> jobs = getJobs(whereClause);
    return jobs.size() == 0 ? null : jobs.get(0);
  }

  public List<Job> getJobsByState(String state, int limit)
      throws ConnectionPoolException, SQLException {
    return getJobs(String.format("WHERE state='%s' ORDER BY ID ASC LIMIT %s", state, limit));
  }

  public List<Job> getJobsByStateAndHost(String state, String host)
      throws ConnectionPoolException, SQLException {
    return getJobs(String.format("WHERE state='%s' And host='%s' ORDER BY ID ASC", state, host));
  }

  public Job getJobById(long id)
      throws ConnectionPoolException, SQLException {
    return getJob(String.format("WHERE id=%s ORDER BY ID ASC", id));
  }

  public List<Job> getJobsByUserId(String userId, int page, int size)
      throws ConnectionPoolException, SQLException {
    return getJobs(String.format(
        "WHERE userId='%s' ORDER BY id DESC LIMIT %s, %s", userId, (page - 1) * size, size));
  }
}

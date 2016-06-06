package com.sogou.beaver.core.execution;

import com.sogou.beaver.core.engine.PrestoEngine;
import com.sogou.beaver.core.engine.SQLEngine;
import com.sogou.beaver.core.plan.ExecutionPlan;
import com.sogou.beaver.dao.JobDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * Created by Tao Li on 2016/6/2.
 */
public class JobExecuteController implements Runnable {
  private final Logger LOG = LoggerFactory.getLogger(JobExecuteController.class);

  private final JobDao dao;
  private final JDBCConnectionPool prestoConnectionPool;
  private volatile boolean isRunning = false;
  private final static String IP = CommonUtils.ip();
  private final static long CHECK_INTERVAL = 3;
  private final static int CHECK_JOB_BATCH = 20;
  private final static long PREEMPT_INTERVAL = 1;
  private final static int JOB_QUEUE_SIZE = 20;
  private final static int WORKER_SIZE = 10;
  private BlockingQueue<Job> jobQueue = new ArrayBlockingQueue<>(JOB_QUEUE_SIZE, true);
  private ExecutorService workerPool = Executors.newFixedThreadPool(WORKER_SIZE);

  public JobExecuteController(JobDao dao, JDBCConnectionPool prestoConnectionPool) {
    this.dao = dao;
    this.prestoConnectionPool = prestoConnectionPool;
  }

  private class Worker implements Runnable {
    @Override
    public void run() {
      while (isRunning && !Thread.currentThread().isInterrupted()) {
        try {
          Job job = jobQueue.take();
          String state = "FAIL";
          try {
            ExecutionPlan plan = ExecutionPlan.fromJson(job.getExecutionPlan());
            switch (plan.getEngine()) {
              case "presto":
                SQLEngine engine = new PrestoEngine(prestoConnectionPool, job.getId());
                try {
                  if (engine.execute(plan.getSql())) {
                    state = "SUCC";
                  }
                } catch (SQLException e) {
                  LOG.error("Failed to execute sql: " + plan.getSql(), e);
                }
              default:
                LOG.error("Not supported engine: " + plan.getEngine());
            }
          } catch (IOException e) {
            LOG.error("Failed to parse executionPlan: " + job.getExecutionPlan(), e);
          }
          job.setState(state);
          job.setEndTime(CommonUtils.now());
          try {
            dao.updateJobById(job, job.getId());
          } catch (ConnectionPoolException | SQLException e) {
            LOG.error("Failed to update job state: " + job.getId());
          }
        } catch (InterruptedException e) {
          LOG.warn("interrupted", e);
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public void run() {
    isRunning = true;

    IntStream.iterate(0, n -> n + 1).limit(WORKER_SIZE).forEach(i -> {
      workerPool.submit(new Worker());
    });

    try {
      cleanZombieJobs();
    } catch (ConnectionPoolException | SQLException e) {
      LOG.error("Failed to clean zombie jobs", e);
    }

    while (isRunning && !Thread.currentThread().isInterrupted()) {
      List<Job> jobs = null;
      try {
        jobs = dao.getJobsByState("WAIT", CHECK_JOB_BATCH);
      } catch (ConnectionPoolException | SQLException e) {
        LOG.error("Failed to get WAIT jobs", e);
      }

      if (jobs != null) {
        jobs.stream().forEach(job -> {
          if (jobQueue.size() < JOB_QUEUE_SIZE && preemptJob(job)) {
            jobQueue.add(job);
          }
        });
      }

      try {
        TimeUnit.SECONDS.sleep(CHECK_INTERVAL);
      } catch (InterruptedException e) {
        LOG.warn("interrupted", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void cleanZombieJobs() throws ConnectionPoolException, SQLException {
    List<Job> lockJobs = dao.getJobsByStateAndHost("LOCK", IP);
    if (lockJobs.size() > 0) {
      dao.updateJobsStateAndHostByIds(
          "WAIT", null, lockJobs.stream().mapToLong(job -> job.getId()).toArray());
    }
    List<Job> runJobs = dao.getJobsByStateAndHost("RUN", IP);
    if (runJobs.size() > 0) {
      dao.updateJobsStateAndHostByIds(
          "FAIL", IP, runJobs.stream().mapToLong(job -> job.getId()).toArray());
    }
  }

  private boolean preemptJob(Job job) {
    try {
      boolean needToRollback = false;

      job.setState("LOCK");
      job.setHost(IP);
      dao.updateJobById(job, job.getId());

      try {
        TimeUnit.SECONDS.sleep(PREEMPT_INTERVAL);
      } catch (InterruptedException e) {
        LOG.warn("interrupted", e);
        Thread.currentThread().interrupt();
        needToRollback = true;
      }

      try {
        job = dao.getJobById(job.getId());
        if (job.getState().equals("LOCK") && job.getHost().equals(IP)) {
          job.setState("RUN");
          dao.updateJobById(job, job.getId());
          return true;
        }
      } catch (ConnectionPoolException | SQLException e) {
        LOG.error("Failed to preempt job: " + job.getId(), e);
        needToRollback = true;
      }

      if (needToRollback) {
        job.setState("WAIT");
        job.setHost(null);
        dao.updateJobByIdAndStateAndHost(job, job.getId(), "LOCK", IP);
      }
    } catch (ConnectionPoolException | SQLException e) {
      LOG.error("Failed to preempt job: " + job.getId(), e);
    }
    return false;
  }

  public void shutdown() {
    isRunning = false;
  }
}

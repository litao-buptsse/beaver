package com.sogou.beaver.core.execution;

import com.sogou.beaver.Config;
import com.sogou.beaver.core.engine.EngineExecutionException;
import com.sogou.beaver.core.engine.PrestoEngine;
import com.sogou.beaver.core.engine.SQLEngine;
import com.sogou.beaver.core.engine.SparkSQLEngine;
import com.sogou.beaver.core.plan.ExecutionPlan;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.common.CommonUtils;
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

  private volatile boolean isRunning = false;
  private final static long CHECK_INTERVAL = 3;
  private final static int CHECK_JOB_BATCH = 20;
  private final static long PREEMPT_INTERVAL = 1;

  private int jobQueueSize;
  private int workerNum;
  private BlockingQueue<Job> jobQueue;
  private ExecutorService workerPool;
  private final String host;

  public JobExecuteController(int jobQueueSize, int workerNum, String host) {
    this.jobQueueSize = jobQueueSize;
    this.workerNum = workerNum;
    this.host = host;
    jobQueue = new ArrayBlockingQueue<>(jobQueueSize, true);
    workerPool = Executors.newFixedThreadPool(workerNum);
  }

  private SQLEngine getSQLEngine(String name, long jobId) {
    switch (name.toUpperCase()) {
      case Config.SQL_ENGINE_PRESTO:
        return new PrestoEngine(Config.PRESTO_POOL, jobId);
      case Config.SQL_ENGINE_SPARK_SQL:
        return new SparkSQLEngine(jobId);
      default:
        return null;
    }
  }

  private class Worker implements Runnable {
    private void runJob(Job job) {
      String state = "FAIL";

      try {
        ExecutionPlan plan = CommonUtils.fromJson(job.getExecutionPlan(), ExecutionPlan.class);
        SQLEngine engine = getSQLEngine(plan.getEngine(), job.getId());
        if (engine != null) {
          try {
            if (engine.execute(plan.getSql(), plan.getInfo())) {
              state = "SUCC";
            }
          } catch (EngineExecutionException e) {
            LOG.error("Failed to execute sql: " + plan.getSql(), e);
          }
        } else {
          LOG.error("Not supported engine: " + plan.getEngine());
        }
      } catch (IOException e) {
        LOG.error("Failed to parse executionPlan: " + job.getExecutionPlan(), e);
      }

      job.setState(state);
      job.setEndTime(CommonUtils.now());
      try {
        Config.JOB_DAO.updateJobById(job, job.getId());
      } catch (ConnectionPoolException | SQLException e) {
        LOG.error("Failed to update job state: " + job.getId());
      }
    }

    @Override
    public void run() {
      while (isRunning && !Thread.currentThread().isInterrupted()) {
        try {
          Job job = jobQueue.take();
          runJob(job);
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

    IntStream.iterate(0, n -> n + 1).limit(workerNum).forEach(i -> {
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
        jobs = Config.JOB_DAO.getJobsByState("WAIT", CHECK_JOB_BATCH);
      } catch (ConnectionPoolException | SQLException e) {
        LOG.error("Failed to get WAIT jobs", e);
      }

      if (jobs != null) {
        jobs.stream().forEach(job -> {
          if (jobQueue.size() < jobQueueSize && preemptJob(job)) {
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
    List<Job> lockJobs = Config.JOB_DAO.getJobsByStateAndHost("LOCK", host);
    if (lockJobs.size() > 0) {
      Config.JOB_DAO.updateJobsStateAndHostByIds(
          "WAIT", null, lockJobs.stream().mapToLong(job -> job.getId()).toArray());
    }
    List<Job> runJobs = Config.JOB_DAO.getJobsByStateAndHost("RUN", host);
    if (runJobs.size() > 0) {
      Config.JOB_DAO.updateJobsStateAndHostByIds(
          "FAIL", host, runJobs.stream().mapToLong(job -> job.getId()).toArray());
    }
  }

  private boolean preemptJob(Job job) {
    try {
      boolean needToRollback = false;

      job.setState("LOCK");
      job.setHost(host);
      Config.JOB_DAO.updateJobById(job, job.getId());

      try {
        TimeUnit.SECONDS.sleep(PREEMPT_INTERVAL);
      } catch (InterruptedException e) {
        LOG.warn("interrupted", e);
        Thread.currentThread().interrupt();
        needToRollback = true;
      }

      try {
        job = Config.JOB_DAO.getJobById(job.getId());
        if (job.getState().equals("LOCK") && job.getHost().equals(host)) {
          job.setState("RUN");
          Config.JOB_DAO.updateJobById(job, job.getId());
          return true;
        }
      } catch (ConnectionPoolException | SQLException e) {
        LOG.error("Failed to preempt job: " + job.getId(), e);
        needToRollback = true;
      }

      if (needToRollback) {
        job.setState("WAIT");
        job.setHost(null);
        Config.JOB_DAO.updateJobByIdAndStateAndHost(job, job.getId(), "LOCK", host);
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

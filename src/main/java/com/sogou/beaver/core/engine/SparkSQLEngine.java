package com.sogou.beaver.core.engine;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/6/17.
 */
public class SparkSQLEngine implements SQLEngine {
  private final long jobId;

  public SparkSQLEngine(long jobId) {
    this.jobId = jobId;
  }

  @Override
  public boolean execute(String sql) throws EngineExecutionException {
    // TODO support dynamic config spark sql executor resources by input volume and sql complexity
    String command = String.format(
        "echo \"%s\" | bin/spark_sql_engine.sh %s >logs/jobs/%s.log 2>&1",
        sql, jobId, jobId);
    ProcessBuilder builder = new ProcessBuilder("bin/runner.py", command);
    Process process = null;
    try {
      process = builder.start();
      int ret = process.waitFor();
      return ret == 0;
    } catch (IOException | InterruptedException e) {
      if (process != null) {
        process.destroy();
      }
      throw new EngineExecutionException(e);
    }
  }
}

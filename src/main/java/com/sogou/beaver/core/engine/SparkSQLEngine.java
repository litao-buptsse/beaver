package com.sogou.beaver.core.engine;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Tao Li on 2016/6/17.
 */
public class SparkSQLEngine implements SQLEngine {
  private final static int DEFAULT_SPARK_EXECUTOR_NUM = 2;
  private final static int MAX_SPARK_EXECUTOR_NUM = 50;

  private final long jobId;

  public SparkSQLEngine(long jobId) {
    this.jobId = jobId;
  }

  private int getSparkExecutorNum(Map<String, String> info) {
    int num = DEFAULT_SPARK_EXECUTOR_NUM;
    if (info.containsKey("spark.executor.instances")) {
      num = Integer.parseInt(info.get("spark.executor.instances"));
    }
    return num < MAX_SPARK_EXECUTOR_NUM ? num : MAX_SPARK_EXECUTOR_NUM;
  }

  @Override
  public boolean execute(String sql, Map<String, String> info) throws EngineExecutionException {
    String command = String.format(
        "echo \"%s\" | bin/spark_sql_engine.sh %s %s >logs/jobs/%s.log 2>&1",
        sql, jobId, getSparkExecutorNum(info), jobId);
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

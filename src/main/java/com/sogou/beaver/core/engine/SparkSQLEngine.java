package com.sogou.beaver.core.engine;

import com.sogou.beaver.Config;
import com.sogou.beaver.common.CommonUtils;
import com.sogou.beaver.common.StreamCollector;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by Tao Li on 2016/6/17.
 */
public class SparkSQLEngine implements SQLEngine {
  private final long jobId;

  public SparkSQLEngine(long jobId) {
    this.jobId = jobId;
  }

  private int getSparkExecutorNum(String sql, Map<String, String> info) {
    int num = Config.DEFAULT_SPARK_EXECUTOR_NUM;
    if (info.containsKey(Config.SPARK_EXECUTOR_NUM_CONFIG)) {
      num = Integer.parseInt(info.get(Config.SPARK_EXECUTOR_NUM_CONFIG));
    } else {
      StreamCollector collector = new StreamCollector();
      String command = String.format("echo \"%s\" | bin/ext/get_spark_executor_num.sh %s %s %s",
          sql, Config.SPARK_EXECUTOR_NUM_FACTOR,
          Config.DEFAULT_SPARK_EXECUTOR_NUM, Config.MAX_SPARK_EXECUTOR_NUM);
      try {
        CommonUtils.runProcess(command, collector, null);
        List<String> output = collector.getOutput();
        if (output.size() == 1 && CommonUtils.isNumeric(output.get(0))) {
          num = Integer.valueOf(output.get(0));
        }
      } catch (IOException e) {
        // ignore
      }
    }
    return num;
  }

  @Override
  public boolean execute(String sql, Map<String, String> info) throws EngineExecutionException {
    String command = String.format(
        "echo \"%s\" | bin/ext/spark_sql_engine.sh %s %s >logs/jobs/%s.log 2>&1",
        sql, jobId, getSparkExecutorNum(sql, info), jobId);
    try {
      return CommonUtils.runProcess(command) == 0;
    } catch (IOException e) {
      throw new EngineExecutionException(e);
    }
  }
}

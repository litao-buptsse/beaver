package com.sogou.beaver;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.sogou.beaver.dao.*;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.util.CommonUtils;
import io.dropwizard.Configuration;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Tao Li on 5/31/16.
 */
public class Config extends Configuration {
  private Map<String, String> beaverConf = Collections.emptyMap();
  private Map<String, String> beaverDBConf = Collections.emptyMap();
  private Map<String, String> prestoConf = Collections.emptyMap();

  @JsonProperty("beaver")
  public Map<String, String> getBeaverConf() {
    return beaverConf;
  }

  @JsonProperty("beaver")
  public void setBeaverConf(Map<String, String> conf) {
    beaverConf = buildConfiguration(conf);
  }

  @JsonProperty("beaverDB")
  public Map<String, String> getBeaverDBConf() {
    return beaverDBConf;
  }

  @JsonProperty("beaverDB")
  public void setBeaverDBConf(Map<String, String> conf) {
    beaverDBConf = buildConfiguration(conf);
  }

  @JsonProperty("presto")
  public Map<String, String> getPrestoConf() {
    return prestoConf;
  }

  @JsonProperty("presto")
  public void setPrestoConf(Map<String, String> conf) {
    prestoConf = buildConfiguration(conf);
  }

  private Map<String, String> buildConfiguration(Map<String, String> conf) {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : conf.entrySet()) {
      builder.put(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  public final static String QUERY_TYPE_RAW = "RAW";
  public final static String QUERY_TYPE_COMPOUND = "COMPOUND";
  public final static String SQL_ENGINE_PRESTO = "PRESTO";
  public final static String SQL_ENGINE_SPARK_SQL = "SPARK-SQL";
  public final static String SPARK_EXECUTOR_NUM = "spark.executor.instances";
  public final static double SPARK_EXECUTOR_NUM_FACTOR = 1.5;
  public final static String FILTER_TYPE_WHERE = "WHERE";
  public final static String FILTER_TYPE_HAVING = "HAVING";

  public static String FILE_OUTPUT_COLLECTOR_ROOT_DIR;
  public static int JOB_QUEUE_SIZE;
  public static int WORKER_NUM;
  public static String HOST;
  public static int MAX_RESULT_RECORD_NUM;

  public static JDBCConnectionPool POOL;
  public static JobDao JOB_DAO;
  public static TableInfoDao TABLE_INFO_DAO;
  public static FieldInfoDao FIELD_INFO_DAO;
  public static EnumInfoDao ENUM_INFO_DAO;
  public static MethodInfoDao METHOD_INFO_DAO;
  public static EngineInfoDao ENGINE_INFO_DAO;
  public static JDBCConnectionPool PRESTO_POOL;

  public static void initStaticConfig(Config conf) throws ConnectionPoolException {
    Map<String, String> beaverConf = conf.getBeaverConf();
    Map<String, String> beaverDBConf = conf.getBeaverDBConf();
    Map<String, String> prestoConf = conf.getPrestoConf();

    FILE_OUTPUT_COLLECTOR_ROOT_DIR = beaverConf.getOrDefault("fileOutputCollectorRootDir", "data");
    JOB_QUEUE_SIZE = Integer.parseInt(beaverConf.getOrDefault("jobQueueSize", "20"));
    WORKER_NUM = Integer.parseInt(beaverConf.getOrDefault("workerNum", "10"));
    HOST = beaverConf.getOrDefault("host", CommonUtils.ip());
    MAX_RESULT_RECORD_NUM = Integer.parseInt(beaverConf.getOrDefault("maxResultRecordNum", "10000"));

    // init db connection pool
    POOL = constructJDBCConnectionPool(beaverDBConf);

    // init dao object
    JOB_DAO = new JobDao();
    TABLE_INFO_DAO = new TableInfoDao();
    FIELD_INFO_DAO = new FieldInfoDao();
    ENUM_INFO_DAO = new EnumInfoDao();
    METHOD_INFO_DAO = new MethodInfoDao();
    ENGINE_INFO_DAO = new EngineInfoDao();

    // init presto connection pool
    Properties info = new Properties();
    info.setProperty("user", "beaver");
    PRESTO_POOL = constructJDBCConnectionPool(prestoConf, info);
  }

  private static JDBCConnectionPool constructJDBCConnectionPool(Map<String, String> conf)
      throws ConnectionPoolException {
    return constructJDBCConnectionPool(conf, new Properties());
  }

  private static JDBCConnectionPool constructJDBCConnectionPool(
      Map<String, String> conf, Properties info) throws ConnectionPoolException {
    String driver = conf.get("driverClass");
    String url = conf.get("url");
    JDBCConnectionPool pool = new JDBCConnectionPool(driver, url, info);

    if (conf.containsKey("initConnectionNum")) {
      pool.setInitConnectionNum(Integer.parseInt(conf.get("initConnectionNum")));
    }
    if (conf.containsKey("minConnectionNum")) {
      pool.setMinConnectionNum(Integer.parseInt(conf.get("minConnectionNum")));
    }
    if (conf.containsKey("maxConnectionNum")) {
      pool.setMaxConnectionNum(Integer.parseInt(conf.get("maxConnectionNum")));
    }
    if (conf.containsKey("idleTimeout")) {
      pool.setIdleTimeout(Long.parseLong(conf.get("idleTimeout")));
    }
    if (conf.containsKey("idleQueueSize")) {
      pool.setIdleQueueSize(Integer.parseInt(conf.get("idleQueueSize")));
    }
    if (conf.containsKey("idleConnectionCloseThreadPoolSize")) {
      pool.setIdleConnectionCloseThreadPoolSize(
          Integer.parseInt(conf.get("idleConnectionCloseThreadPoolSize")));
    }

    return pool;
  }
}

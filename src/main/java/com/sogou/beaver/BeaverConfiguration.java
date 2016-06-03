package com.sogou.beaver;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.sogou.beaver.db.ConnectionPool;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import io.dropwizard.Configuration;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Tao Li on 5/31/16.
 */
public class BeaverConfiguration extends Configuration {
  private Map<String, Map<String, String>> mysqlConfiguration = Collections.emptyMap();

  @JsonProperty("mysql")
  public Map<String, Map<String, String>> getMysqlConfiguration() {
    return mysqlConfiguration;
  }

  @JsonProperty("mysql")
  public void setMysqlConfiguration(Map<String, Map<String, String>> configuration) {
    final ImmutableMap.Builder<String, Map<String, String>> builder = ImmutableMap.builder();
    for (Map.Entry<String, Map<String, String>> entry : configuration.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    this.mysqlConfiguration = builder.build();
  }

  private Map<String, Map<String, String>> prestoConfiguration = Collections.emptyMap();

  @JsonProperty("presto")
  public Map<String, Map<String, String>> getPrestoConfiguration() {
    return prestoConfiguration;
  }

  @JsonProperty("presto")
  public void setPrestoConfiguration(Map<String, Map<String, String>> configuration) {
    final ImmutableMap.Builder<String, Map<String, String>> builder = ImmutableMap.builder();
    for (Map.Entry<String, Map<String, String>> entry : configuration.entrySet()) {
      builder.put(entry.getKey(), ImmutableMap.copyOf(entry.getValue()));
    }
    this.prestoConfiguration = builder.build();
  }

  public JDBCConnectionPool constructJDBCConnectionPool(
      Map<String, Map<String, String>> configuration) throws ConnectionPoolException {
    return constructJDBCConnectionPool(configuration, new Properties());
  }

  public JDBCConnectionPool constructJDBCConnectionPool(
      Map<String, Map<String, String>> configuration, Properties info)
      throws ConnectionPoolException {
    String driver = configuration.get("connectionPool").get("driverClass");
    String url = configuration.get("connectionPool").get("url");
    JDBCConnectionPool pool = new JDBCConnectionPool(driver, url, info);
    configConnectionPool(pool, configuration.get("connectionPool"));
    return pool;
  }

  private void configConnectionPool(ConnectionPool pool, Map<String, String> configuration) {
    if (configuration.containsKey("initConnectionNum")) {
      pool.setInitConnectionNum(
          Integer.parseInt(configuration.get("initConnectionNum")));
    }
    if (configuration.containsKey("minConnectionNum")) {
      pool.setMinConnectionNum(
          Integer.parseInt(configuration.get("minConnectionNum")));
    }
    if (configuration.containsKey("maxConnectionNum")) {
      pool.setMaxConnectionNum(
          Integer.parseInt(configuration.get("maxConnectionNum")));
    }
    if (configuration.containsKey("idleTimeout")) {
      pool.setIdleTimeout(
          Long.parseLong(configuration.get("idleTimeout")));
    }
    if (configuration.containsKey("idleQueueSize")) {
      pool.setIdleQueueSize(
          Integer.parseInt(configuration.get("idleQueueSize")));
    }
    if (configuration.containsKey("idleConnectionCloseThreadPoolSize")) {
      pool.setIdleConnectionCloseThreadPoolSize(
          Integer.parseInt(configuration.get("idleConnectionCloseThreadPoolSize")));
    }
  }
}

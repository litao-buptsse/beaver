package com.sogou.beaver.core.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sogou.beaver.dao.TableInfoDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.TableInfo;
import com.sogou.beaver.util.CommonUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Tao Li on 6/19/16.
 */
public class CompoundQuery implements Query {
  private String tableName;
  private List<Metric> metrics;
  private List<Bucket> buckets;
  private List<Filter> filters;
  private TimeRange timeRange;

  static class Metric {
    private String method;
    private String field;
    private String alias;

    public Metric() {
    }

    public Metric(String method, String field, String alias) {
      this.method = method;
      this.field = field;
      this.alias = alias;
    }

    public String getAlias() {
      return alias != null && !alias.equals("") ? alias : String.format("%s_%s", method, field);
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }

    public String getMethod() {
      return method;
    }

    public void setMethod(String method) {
      this.method = method;
    }
  }

  static class Bucket {
    private String field;
    private String alias;

    public Bucket() {
    }

    public Bucket(String field, String alias) {
      this.field = field;
      this.alias = alias;
    }

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }

    public String getAlias() {
      return alias != null && !alias.equals("") ? alias : field;
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }
  }

  static class Filter {
    private String method;
    private String field;
    private String value;

    public Filter() {
    }

    public Filter(String method, String field, String value) {
      this.method = method;
      this.field = field;
      this.value = value;
    }

    public String getMethod() {
      return method;
    }

    public void setMethod(String method) {
      this.method = method;
    }

    public String getField() {
      return field;
    }

    public void setField(String field) {
      this.field = field;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  static class TimeRange {
    private String startTime;
    private String endTime;

    public TimeRange() {
    }

    public TimeRange(String startTime, String endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public String getStartTime() {
      return startTime;
    }

    public void setStartTime(String startTime) {
      this.startTime = startTime;
    }

    public String getEndTime() {
      return endTime;
    }

    public void setEndTime(String endTime) {
      this.endTime = endTime;
    }
  }

  public CompoundQuery() {
  }

  public CompoundQuery(String tableName,
                       List<Metric> metrics, List<Bucket> buckets, List<Filter> filters,
                       TimeRange timeRange) {
    this.tableName = tableName;
    this.metrics = metrics;
    this.buckets = buckets;
    this.filters = filters;
    this.timeRange = timeRange;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<Metric> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<Metric> metrics) {
    this.metrics = metrics;
  }

  public List<Bucket> getBuckets() {
    return buckets;
  }

  public void setBuckets(List<Bucket> buckets) {
    this.buckets = buckets;
  }

  public List<Filter> getFilters() {
    return filters;
  }

  public void setFilters(List<Filter> filters) {
    this.filters = filters;
  }

  public TimeRange getTimeRange() {
    return timeRange;
  }

  public void setTimeRange(TimeRange timeRange) {
    this.timeRange = timeRange;
  }

  public static CompoundQuery fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), CompoundQuery.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }

  private static long getTimeIntervalMinutes(String startTime, String endTime, String frequency) {
    String timeFormat;
    switch (frequency) {
      case "DAY":
        timeFormat = "yyyyMMdd";
        break;
      case "HOUR":
        timeFormat = "yyyyMMddHH";
        break;
      case "5MIN":
        timeFormat = "yyyyMMddHHmm";
        break;
      default:
        return -1;
    }

    long startTimestamp = CommonUtils.convertStringToTimestamp(startTime, timeFormat);
    long endTimestamp = CommonUtils.convertStringToTimestamp(endTime, timeFormat);
    return (endTimestamp - startTimestamp) / 1000 / 60 + 60;
  }

  @Override
  public String parseEngine(JDBCConnectionPool pool) {
    try {
      TableInfoDao dao = new TableInfoDao(pool);
      TableInfo tableInfo = dao.getTableInfoByName(tableName);
      if (tableInfo != null) {
        long timeIntervalMinutes = getTimeIntervalMinutes(
            timeRange.getStartTime(),
            timeRange.getEndTime(),
            tableInfo.getFrequency());
        if (tableName.startsWith("custom.")
            && timeIntervalMinutes != -1 && timeIntervalMinutes <= 1440) {
          return "presto";
        }
      }
    } catch (ConnectionPoolException | SQLException e) {
      // ignore
    }
    return "spark-sql";
  }

  // TODO support more complex compound query
  @Override
  public String parseSQL(JDBCConnectionPool pool) {
    String metricSQL = metrics.stream()
        .map(metric -> String.format("%s AS %s",
            parseMetric(metric.getMethod(), metric.getField()), metric.getAlias()))
        .collect(Collectors.joining(", "));
    String bucketMetricSQL = buckets.stream()
        .map(bucket -> String.format("%s AS %s",
            bucket.getField(),
            bucket.getAlias()))
        .collect(Collectors.joining(", "));
    String timeRangeSQL = String.format("logdate>=%s AND logdate<=%s",
        CommonUtils.formatSQLValue(timeRange.getStartTime()),
        CommonUtils.formatSQLValue(timeRange.getEndTime()));
    String filterSQL = filters.stream()
        .map(filter -> parseFilter(
            filter.getMethod(), filter.getField(), filter.getValue()))
        .collect(Collectors.joining(" AND "));
    String bucketSQL = buckets.stream()
        .map(bucket -> bucket.getField())
        .collect(Collectors.joining(", "));

    String sql = String.format("SELECT %s, %s FROM %s WHERE %s AND %s GROUP BY %s",
        bucketMetricSQL, metricSQL, tableName, timeRangeSQL, filterSQL, bucketSQL);
    return sql;
  }

  private static String parseMetric(String method, String field) {
    switch (method) {
      case "count_distinct":
        return String.format("count(DISTINCT %s)", field);
      default:
        return String.format("%s(%s)", method, field);
    }
  }

  private static String parseFilter(String method, String field, String value) {
    String realMethod = method;
    String realValue = CommonUtils.formatSQLValue(value);
    switch (method) {
      case "eq":
        realMethod = "=";
        break;
      case "ne":
        realMethod = "!=";
        break;
      case "gt":
        realMethod = ">";
        break;
      case "ge":
        realMethod = ">=";
        break;
      case "lt":
        realMethod = "<";
        break;
      case "le":
        realMethod = "<=";
        break;
      case "in":
        realMethod = "IN";
        realValue = String.format("(%s)", Stream.of(value.split(","))
            .map(v -> CommonUtils.formatSQLValue(v.trim())).collect(Collectors.joining(", ")));
        break;
      case "not_in":
        realMethod = "NOT IN";
        realValue = String.format("(%s)", Stream.of(value.split(","))
            .map(v -> CommonUtils.formatSQLValue(v.trim())).collect(Collectors.joining(", ")));
        break;
    }
    return String.format("%s %s %s", field, realMethod, realValue);
  }
}
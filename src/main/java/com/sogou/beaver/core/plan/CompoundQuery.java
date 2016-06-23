package com.sogou.beaver.core.plan;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.TableInfo;
import com.sogou.beaver.util.CommonUtils;

import java.sql.SQLException;
import java.util.*;
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
    private String filterType;
    private String dataType;
    private String method;
    private String field;
    private String value;

    public Filter() {
    }

    public Filter(String filterType, String dataType, String method, String field, String value) {
      this.filterType = filterType;
      this.dataType = dataType;
      this.method = method;
      this.field = field;
      this.value = value;
    }

    public String getFilterType() {
      return filterType;
    }

    public void setFilterType(String filterType) {
      this.filterType = filterType;
    }

    public String getDataType() {
      return dataType;
    }

    public void setDataType(String dataType) {
      this.dataType = dataType;
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
  public String parseEngine() throws ParseException {
    try {
      TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoByName(tableName);
      if (tableInfo != null) {
        long timeIntervalMinutes = getTimeIntervalMinutes(
            timeRange.getStartTime(), timeRange.getEndTime(), tableInfo.getFrequency());
        if (tableName.startsWith("custom.")
            && timeIntervalMinutes != -1 && timeIntervalMinutes <= 1440) {
          return Config.SQL_ENGINE_PRESTO;
        }
      }
    } catch (ConnectionPoolException | SQLException e) {
      // ignore
    }
    return Config.SQL_ENGINE_SPARK_SQL;
  }

  // TODO support more complex compound query
  @Override
  public String parseSQL() throws ParseException {
    String bucketMetricSQL = buckets.stream()
        .map(bucket -> String.format("%s AS %s",
            bucket.getField(),
            bucket.getAlias()))
        .collect(Collectors.joining(", "));

    String metricSQL = metrics.stream()
        .map(metric -> String.format("%s AS %s",
            parseMetric(metric.getMethod(), metric.getField()), metric.getAlias()))
        .collect(Collectors.joining(", "));
    metricSQL = !metricSQL.equals("") && !bucketMetricSQL.equals("") ? ", " + metricSQL : metricSQL;

    if (bucketMetricSQL.equals("") && metricSQL.equals("")) {
      throw new ParseException("Nothing to select");
    }

    if (timeRange.getEndTime().compareTo(timeRange.getStartTime()) < 0) {
      throw new ParseException("EndTime is less than startTime");
    }

    String timeRangeSQL = String.format("logdate>=%s AND logdate<=%s",
        CommonUtils.formatSQLValue(timeRange.getStartTime()),
        CommonUtils.formatSQLValue(timeRange.getEndTime()));

    String whereSQL = filters.stream()
        .filter(filter -> filter.getFilterType().equalsIgnoreCase(Config.FILTER_TYPE_WHERE))
        .map(filter -> parseFilter(
            filter.getMethod(), filter.getField(), filter.getValue(), filter.getDataType()))
        .collect(Collectors.joining(" AND "));
    whereSQL = !whereSQL.equals("") ? "AND " + whereSQL : "";

    String bucketSQL = buckets.stream()
        .map(bucket -> bucket.getField())
        .collect(Collectors.joining(", "));
    bucketSQL = !bucketSQL.equals("") ? "GROUP BY " + bucketSQL : "";

    String havingSQL = filters.stream()
        .filter(filter -> filter.getFilterType().equalsIgnoreCase(Config.FILTER_TYPE_HAVING))
        .filter(filter -> filter.getField().indexOf(":") > 0)
        .map(filter -> parseFilter(filter.getMethod(),
            parseHavingField(filter.getField()), filter.getValue(), filter.getDataType()))
        .collect(Collectors.joining(" AND "));
    havingSQL = !havingSQL.equals("") ? "HAVING " + havingSQL : "";

    if (bucketSQL.equals("") && (!bucketMetricSQL.equals("") || !havingSQL.equals(""))) {
      throw new ParseException("Can't call aggregate function when nothing to group by");
    }

    String orderBySQL = metrics.stream().map(metric -> metric.getAlias() + " DESC")
        .collect(Collectors.joining(", "));
    orderBySQL = !orderBySQL.equals("") ? "ORDER BY " + orderBySQL : "";

    String limitSQL = "LIMIT " + Config.MAX_RESULT_RECORD_NUM;

    String sql = String.format("SELECT %s %s FROM %s WHERE %s %s %s %s %s %s",
        bucketMetricSQL, metricSQL, tableName, timeRangeSQL, whereSQL, bucketSQL, havingSQL,
        orderBySQL, limitSQL);
    return sql;
  }

  @Override
  public Map<String, String> parseInfo() throws ParseException {
    Map<String, String> info = new HashMap<>();
    if (parseEngine().equals(Config.SQL_ENGINE_SPARK_SQL)) {
      double SPARK_EXECUTOR_NUM_FACTOR = 1.5;
      try {
        TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoByName(tableName);
        if (tableInfo != null) {
          long timeIntervalMinutes = getTimeIntervalMinutes(
              timeRange.getStartTime(), timeRange.getEndTime(), tableInfo.getFrequency());
          int sparkExecutorNum = (int) (timeIntervalMinutes / 60 * SPARK_EXECUTOR_NUM_FACTOR);
          info.put(Config.CONF_SPARK_EXECUTOR_NUM, String.valueOf(sparkExecutorNum));
        }
      } catch (ConnectionPoolException | SQLException e) {
        // ignore
      }
    }
    return info;
  }

  private static String parseMetric(String method, String field) {
    switch (method) {
      case "count_distinct":
        return String.format("count(DISTINCT %s)", field);
      default:
        return String.format("%s(%s)", method, field);
    }
  }

  private static String parseHavingField(String field) {
    int i = field.indexOf(":");
    return parseMetric(field.substring(0, i), field.substring(i + 1, field.length()));
  }

  private static String parseFilter(String method, String field, String value, String dataType) {
    method = method.toUpperCase();
    dataType = dataType.toUpperCase();

    List<String> dataTypes = Arrays.asList("INT", "LONG", "FLOAT", "DOUBLE", "BOOLEAN");
    boolean withQuotes = dataTypes.contains(dataType);

    Map<String, String> filterMethods = new HashMap<String, String>() {{
      put("EQ", "=");
      put("NE", "!=");
      put("GT", ">");
      put("GE", ">=");
      put("LT", "<");
      put("LE", "<=");
      put("IN", "IN");
      put("NOT_IN", "NOT IN");
    }};

    String realMethod = method;
    if (filterMethods.containsKey(method)) {
      realMethod = filterMethods.get(method);
    }

    String realValue = CommonUtils.formatSQLValue(value, withQuotes);
    if (method.equalsIgnoreCase("IN") || method.equalsIgnoreCase("NOT_IN")) {
      realValue = String.format("(%s)", Stream.of(value.split(","))
          .map(v -> CommonUtils.formatSQLValue(v.trim(), withQuotes)).collect(Collectors.joining(", ")));
    }

    return String.format("%s %s %s", field, realMethod, realValue);
  }
}
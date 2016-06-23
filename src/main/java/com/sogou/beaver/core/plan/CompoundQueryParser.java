package com.sogou.beaver.core.plan;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.TableInfo;
import com.sogou.beaver.util.CommonUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Tao Li on 6/19/16.
 */
public class CompoundQueryParser {
  public static String parseEngine(CompoundQuery query) throws ParseException {
    try {
      TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoByName(query.getTableName());
      if (tableInfo != null) {
        long timeIntervalMinutes = CompoundQueryParser.getTimeIntervalMinutes(
            query.getTimeRange().getStartTime(),
            query.getTimeRange().getEndTime(),
            tableInfo.getFrequency()
        );
        if (query.getTableName().startsWith("custom.")
            && timeIntervalMinutes != -1 && timeIntervalMinutes <= 1440) {
          return Config.SQL_ENGINE_PRESTO;
        }
      }
    } catch (ConnectionPoolException | SQLException e) {
      // ignore
    }
    return Config.SQL_ENGINE_SPARK_SQL;
  }

  public static String parseSQL(CompoundQuery query) throws ParseException {
    String bucketMetricSQL = query.getBuckets().stream()
        .map(bucket -> String.format("%s AS %s", bucket.getField(), bucket.getAlias()))
        .collect(Collectors.joining(", "));

    String metricSQL = query.getMetrics().stream()
        .map(metric ->
            String.format("%s AS %s",
                CompoundQueryParser.parseMetric(metric.getMethod(), metric.getField()),
                metric.getAlias())
        )
        .collect(Collectors.joining(", "));
    metricSQL = !metricSQL.equals("") && !bucketMetricSQL.equals("") ? ", " + metricSQL : metricSQL;

    if (bucketMetricSQL.equals("") && metricSQL.equals("")) {
      throw new ParseException("Nothing to select");
    }

    if (query.getTimeRange().getEndTime().compareTo(query.getTimeRange().getStartTime()) < 0) {
      throw new ParseException("EndTime is less than startTime");
    }

    String timeRangeSQL = String.format("logdate>=%s AND logdate<=%s",
        CommonUtils.formatSQLValue(query.getTimeRange().getStartTime(), true),
        CommonUtils.formatSQLValue(query.getTimeRange().getEndTime(), true));

    String whereSQL = query.getFilters().stream()
        .filter(filter -> filter.getFilterType().equalsIgnoreCase(Config.FILTER_TYPE_WHERE))
        .map(filter ->
            CompoundQueryParser.parseFilter(
                filter.getMethod(),
                filter.getField(),
                filter.getValue(),
                filter.getDataType())
        )
        .collect(Collectors.joining(" AND "));
    whereSQL = !whereSQL.equals("") ? "AND " + whereSQL : "";

    String bucketSQL = query.getBuckets().stream()
        .map(bucket -> bucket.getField())
        .collect(Collectors.joining(", "));
    bucketSQL = !bucketSQL.equals("") ? "GROUP BY " + bucketSQL : "";

    String havingSQL = query.getFilters().stream()
        .filter(filter -> filter.getFilterType().equalsIgnoreCase(Config.FILTER_TYPE_HAVING))
        .filter(filter -> filter.getField().indexOf(":") > 0)
        .map(filter ->
            CompoundQueryParser.parseFilter(
                filter.getMethod(),
                CompoundQueryParser.parseHavingField(filter.getField()),
                filter.getValue(),
                filter.getDataType())
        )
        .collect(Collectors.joining(" AND "));
    havingSQL = !havingSQL.equals("") ? "HAVING " + havingSQL : "";

    if (bucketSQL.equals("") && (!bucketMetricSQL.equals("") || !havingSQL.equals(""))) {
      throw new ParseException("Can't call aggregate function when nothing to group by");
    }

    String orderBySQL = query.getMetrics().stream()
        .map(metric -> metric.getAlias() + " DESC")
        .collect(Collectors.joining(", "));
    orderBySQL = !orderBySQL.equals("") ? "ORDER BY " + orderBySQL : "";

    String limitSQL = "LIMIT " + Config.MAX_RESULT_RECORD_NUM;

    String sql = String.format("SELECT %s %s FROM %s WHERE %s %s %s %s %s %s",
        bucketMetricSQL, metricSQL, query.getTableName(), timeRangeSQL, whereSQL, bucketSQL,
        havingSQL, orderBySQL, limitSQL);
    return sql;
  }

  public static Map<String, String> parseInfo(CompoundQuery query) throws ParseException {
    Map<String, String> info = new HashMap<>();
    if (parseEngine(query).equals(Config.SQL_ENGINE_SPARK_SQL)) {
      double SPARK_EXECUTOR_NUM_FACTOR = 1.5;
      try {
        TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoByName(query.getTableName());
        if (tableInfo != null) {
          long timeIntervalMinutes = CompoundQueryParser.getTimeIntervalMinutes(
              query.getTimeRange().getStartTime(),
              query.getTimeRange().getEndTime(),
              tableInfo.getFrequency()
          );
          int sparkExecutorNum = (int) (timeIntervalMinutes / 60 * SPARK_EXECUTOR_NUM_FACTOR);
          info.put(Config.CONF_SPARK_EXECUTOR_NUM, String.valueOf(sparkExecutorNum));
        }
      } catch (ConnectionPoolException | SQLException e) {
        // ignore
      }
    }
    return info;
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

  private static String getArrayField(String field) {
    if (field.contains(".")) {
      return field.substring(0, field.indexOf("."));
    } else {
      return null;
    }
  }

  public static String parseMetric(String method, String field) {
    switch (method) {
      case "count_distinct":
        return String.format("count(DISTINCT %s)", field);
      default:
        return String.format("%s(%s)", method, field);
    }
  }

  public static String parseHavingField(String field) {
    int i = field.indexOf(":");
    return parseMetric(field.substring(0, i), field.substring(i + 1, field.length()));
  }

  public static String parseFilter(String method, String field, String value, String dataType) {
    method = method.toUpperCase();
    dataType = dataType.toUpperCase();

    List<String> dataTypes = Arrays.asList("INT", "LONG", "FLOAT", "DOUBLE", "BOOLEAN");
    boolean withQuotes = !dataTypes.contains(dataType);

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
          .map(v -> CommonUtils.formatSQLValue(v.trim(), withQuotes))
          .collect(Collectors.joining(", ")));
    }

    return String.format("%s %s %s", field, realMethod, realValue);
  }
}
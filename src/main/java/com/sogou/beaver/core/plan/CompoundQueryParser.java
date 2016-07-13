package com.sogou.beaver.core.plan;

import com.sogou.beaver.Config;
import com.sogou.beaver.common.CommonUtils;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.TableInfo;
import com.sogou.beaver.model.TableMetric;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Tao Li on 6/19/16.
 */
public class CompoundQueryParser {
  private final static String TABLE_METRIC_PREFIX = "m_";
  private final static String LATERAL_VIEW_FIELD_PREFIX = "f_";

  public static ExecutionPlan parseExecutionPlan(CompoundQuery query) throws ParseException {
    try {
      TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoByName(query.getTableName());
      if (tableInfo == null) {
        throw new ParseException("No such table: " + query.getTableName());
      }

      Map<String, TableMetric> tableMetrics = Config.TABLE_METRIC_DAO
          .getTableMetricsByViewId(tableInfo.getId())
          .stream().collect(Collectors.toMap(TableMetric::getName, Function.identity()));

      String engine = parseEngine(tableInfo, query);
      String sql = parseSQL(tableInfo, tableMetrics, engine, query);
      return new ExecutionPlan(engine, sql, new HashMap<>());
    } catch (ConnectionPoolException | SQLException e) {
      throw new ParseException(e);
    }
  }

  private static String parseEngine(TableInfo tableInfo, CompoundQuery query) {
    String engine = Config.SQL_ENGINE_SPARK_SQL;

    long timeIntervalMinutes = getTimeIntervalMinutes(query.getFrequency(),
        query.getTimeRange().getStartTime(), query.getTimeRange().getEndTime());
    /*
     * Presto Engine Condition:
     * 1. FileFormat: RC/ORC
     * 2. Database: custom
     * 3. Time Range: with a day
     */
    if ((tableInfo.getFileFormat().equalsIgnoreCase(Config.FILE_FORMAT_ORC)
        || tableInfo.getFileFormat().equalsIgnoreCase(Config.FILE_FORMAT_RCFILE))
        && tableInfo.getDatabase().equalsIgnoreCase(Config.HIVE_DATABASE_CUSTOM)
        && timeIntervalMinutes != -1 && timeIntervalMinutes <= 1440) {
      engine = Config.SQL_ENGINE_PRESTO;
    }

    return engine;
  }

  private static String parseSQL(TableInfo tableInfo, Map<String, TableMetric> tableMetrics,
                                 String engine, CompoundQuery query)
      throws ParseException {
    String tableName = tableInfo.getDatabase() + "." + tableInfo.getTableName();
    switch (query.getFrequency()) {
      case "5MIN":
        tableName = tableName + "_5min";
        break;
      case "DAY":
        tableName = tableName + "_day";
        break;
    }

    String lateralViewSQL = parseLateralViewSQL(tableInfo.getExplodeField(), engine);

    String bucketMetricSQL = parseBucketMetricSQL(query.getBuckets());

    String metricSQL = parseMetricSQL(engine, tableMetrics, query.getMetrics());
    metricSQL = !metricSQL.equals("") && !bucketMetricSQL.equals("") ? ", " + metricSQL : metricSQL;

    if (bucketMetricSQL.equals("") && metricSQL.equals("")) {
      throw new ParseException("Nothing to select");
    }

    if (query.getTimeRange().getEndTime().compareTo(query.getTimeRange().getStartTime()) < 0) {
      throw new ParseException("EndTime is less than startTime");
    }

    String timeRangeSQL = parseTimeRangeSQL(
        query.getTimeRange().getStartTime(), query.getTimeRange().getEndTime());

    String whereSQL = parseWhereSQL(tableInfo.getPreFilterSQL(), query.getFilters());
    whereSQL = !whereSQL.equals("") ? "AND " + whereSQL : "";

    String bucketSQL = parseBucketSQL(engine, query.getBuckets());
    bucketSQL = !bucketSQL.equals("") ? "GROUP BY " + bucketSQL : "";

    String havingSQL = parseHavingSQL(engine, tableMetrics, query.getFilters());
    havingSQL = !havingSQL.equals("") ? "HAVING " + havingSQL : "";

    if (bucketSQL.equals("") && (!bucketMetricSQL.equals("") || !havingSQL.equals(""))) {
      throw new ParseException("Can't call aggregate function when nothing to group by");
    }

    String orderBySQL = parseOrderBySQL(query.getMetrics());
    orderBySQL = !orderBySQL.equals("") ? "ORDER BY " + orderBySQL : "";

    String limitSQL = parseLimitSQL();

    return String.format("SELECT %s %s FROM %s %s WHERE %s %s %s %s %s %s",
        bucketMetricSQL, metricSQL, tableName, lateralViewSQL, timeRangeSQL, whereSQL,
        bucketSQL, havingSQL, orderBySQL, limitSQL);
  }

  private static String parseLateralViewSQL(String explodeField, String engine) {
    String sql = "";
    if (explodeField != null) {
      switch (engine) {
        case Config.SQL_ENGINE_PRESTO:
          sql = String.format("CROSS JOIN UNNEST(%s) AS t (%s)",
              explodeField, LATERAL_VIEW_FIELD_PREFIX + explodeField);
          break;
        case Config.SQL_ENGINE_SPARK_SQL:
          sql = String.format("LATERAL VIEW explode(%s) t AS %s",
              explodeField, LATERAL_VIEW_FIELD_PREFIX + explodeField);
          break;
      }
    }
    return sql;
  }

  private static String parseBucketMetricSQL(List<CompoundQuery.Bucket> buckets) {
    return buckets.stream()
        .map(bucket -> String.format("%s AS %s",
            getField(bucket.getField()), bucket.getAlias()))
        .collect(Collectors.joining(", "));
  }

  private static String parseMetricSQL(String engine, Map<String, TableMetric> tableMetrics,
                                       List<CompoundQuery.Metric> metrics) {
    return metrics.stream()
        .map(metric -> String.format("%s AS %s",
            getMetric(engine, metric.getMethod(), getField(metric.getField()),
                tableMetrics.get(metric.getMethod())),
            metric.getAlias()))
        .collect(Collectors.joining(", "));
  }

  private static String parseTimeRangeSQL(String startTime, String endTime) {
    return String.format("logdate>=%s AND logdate<=%s",
        CommonUtils.formatSQLValue(startTime, true), CommonUtils.formatSQLValue(endTime, true));
  }

  private static String parseWhereSQL(String prefilterSQL, List<CompoundQuery.Filter> filters) {
    String whereSQL = filters.stream()
        .filter(filter -> filter.getFilterType().equalsIgnoreCase(Config.FILTER_TYPE_WHERE))
        .map(filter -> getFilter(filter.getMethod(),
            getField(filter.getField()), filter.getValue(), filter.getDataType()))
        .collect(Collectors.joining(" AND "));

    if (prefilterSQL == null) {
      return whereSQL;
    } else {
      if (whereSQL.equals("")) {
        return prefilterSQL;
      } else {
        return prefilterSQL + " AND " + whereSQL;
      }
    }
  }

  private static String parseBucketSQL(String engine, List<CompoundQuery.Bucket> buckets) {
    return buckets.stream().map(bucket -> getField(bucket.getField()))
        .collect(Collectors.joining(", "));
  }

  private static String parseHavingSQL(String engine, Map<String, TableMetric> tableMetrics,
                                       List<CompoundQuery.Filter> filters) {
    return filters.stream()
        .filter(filter -> filter.getFilterType().equalsIgnoreCase(Config.FILTER_TYPE_HAVING))
        .map(filter -> getFilter(
            filter.getMethod(),
            getHavingField(
                engine, getField(filter.getField()), tableMetrics.get(filter.getField())),
            filter.getValue(),
            filter.getDataType()))
        .collect(Collectors.joining(" AND "));
  }

  private static String parseOrderBySQL(List<CompoundQuery.Metric> metrics) {
    return metrics.stream().map(metric -> metric.getAlias() + " DESC")
        .collect(Collectors.joining(", "));
  }

  private static String parseLimitSQL() {
    // return "LIMIT " + Config.MAX_RESULT_RECORD_NUM;
    return "";
  }

  private static long getTimeIntervalMinutes(String frequency, String startTime, String endTime) {
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

  private static String getMetric(String engine, String method, String field,
                                  TableMetric tableMetric) {
    if (method.startsWith(TABLE_METRIC_PREFIX)) {
      if (engine.equalsIgnoreCase(Config.SQL_ENGINE_PRESTO)) {
        return tableMetric.getPrestoExpression();
      } else {
        return tableMetric.getExpression();
      }
    } else {
      if (engine.equalsIgnoreCase(Config.SQL_ENGINE_PRESTO)) {
        List<String> methods = Arrays.asList("sum", "avg", "max", "min");
        if (methods.contains(method)) {
          field = String.format("CAST(%s AS bigint)", field);
        }
      }
      switch (method) {
        case "count_distinct":
          return String.format("count(DISTINCT %s)", field);
        default:
          return String.format("%s(%s)", method, field);
      }
    }
  }

  private static String getField(String field) {
    if (field.contains(".")) {
      return LATERAL_VIEW_FIELD_PREFIX + field;
    }
    return field;
  }

  private static String getHavingField(String engine, String field, TableMetric tableMetric) {
    String method = field;
    String realField = null;
    if (!field.startsWith(TABLE_METRIC_PREFIX)) {
      int i = field.indexOf(":");
      method = field.substring(0, i);
      realField = field.substring(i + 1, field.length());
    }
    return getMetric(engine, method, realField, tableMetric);
  }

  private static String getFilter(String method, String field, String value, String dataType) {
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
      put("LIKE", "LIKE");
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

    String filter = String.format("%s %s %s", field, realMethod, realValue);

    if (method.equalsIgnoreCase("LIKE")) {
      filter = String.format("(%s)", Stream.of(value.split(","))
          .map(v -> field + " LIKE '%" + v.trim() + "%'")
          .collect(Collectors.joining(" OR ")));
    }

    return filter;
  }
}
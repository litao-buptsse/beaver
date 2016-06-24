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
    String engine = parseEngine(query);

    List<String> arrayFields = CompoundQueryParser.parseArrayFields(
        query.getMetrics(), query.getBuckets(), query.getFilters()
    );

    String lateralViewSQL = "";
    if (arrayFields.size() > 0) {
      switch (parseEngine(query)) {
        case Config.SQL_ENGINE_PRESTO:
          lateralViewSQL = String.format("CROSS JOIN UNNEST(%s) AS t (%s)",
              arrayFields.stream().collect(Collectors.joining(", ")),
              arrayFields.stream().map(f -> "f_" + f).collect(Collectors.joining(", "))
          );
          break;
        case Config.SQL_ENGINE_SPARK_SQL:
          lateralViewSQL = arrayFields.stream().map(f ->
              String.format("LATERAL VIEW explode(%s) %s AS %s", f, "t_" + f, "f_" + f)
          ).collect(Collectors.joining(" "));
          break;
      }
    }

    String bucketMetricSQL = query.getBuckets().stream()
        .map(bucket -> String.format("%s AS %s",
            parseField(engine, bucket.getField()), bucket.getAlias()))
        .collect(Collectors.joining(", "));

    String metricSQL = query.getMetrics().stream()
        .map(metric ->
            String.format("%s AS %s",
                CompoundQueryParser.parseMetric(
                    engine, metric.getMethod(), parseField(engine, metric.getField())
                ),
                metric.getAlias()
            )
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
                parseField(engine, filter.getField()),
                filter.getValue(),
                filter.getDataType())
        )
        .collect(Collectors.joining(" AND "));
    whereSQL = !whereSQL.equals("") ? "AND " + whereSQL : "";

    String bucketSQL = query.getBuckets().stream()
        .map(bucket -> parseField(engine, bucket.getField()))
        .collect(Collectors.joining(", "));
    bucketSQL = !bucketSQL.equals("") ? "GROUP BY " + bucketSQL : "";

    String havingSQL = query.getFilters().stream()
        .filter(filter -> filter.getFilterType().equalsIgnoreCase(Config.FILTER_TYPE_HAVING))
        .filter(filter -> filter.getField().indexOf(":") > 0)
        .map(filter ->
            CompoundQueryParser.parseFilter(
                filter.getMethod(),
                CompoundQueryParser.parseHavingField(engine, parseField(engine, filter.getField())),
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

    String sql = String.format("SELECT %s %s FROM %s %s WHERE %s %s %s %s %s %s",
        bucketMetricSQL, metricSQL, query.getTableName(), lateralViewSQL, timeRangeSQL, whereSQL,
        bucketSQL, havingSQL, orderBySQL, limitSQL);
    return sql;
  }

  public static Map<String, String> parseInfo(CompoundQuery query) throws ParseException {
    Map<String, String> info = new HashMap<>();
    if (parseEngine(query).equalsIgnoreCase(Config.SQL_ENGINE_SPARK_SQL)) {
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

  private static String[] getArrayField(String field) {
    if (field.contains(".")) {
      String arrayField = field.substring(0, field.indexOf("."));
      String subField = field.substring(field.indexOf(".") + 1, field.length());
      return new String[]{arrayField, subField};
    } else {
      return null;
    }
  }

  public static List<String> parseArrayFields(List<CompoundQuery.Metric> metrics,
                                              List<CompoundQuery.Bucket> buckets,
                                              List<CompoundQuery.Filter> filters) {
    return Stream.concat(Stream.concat(
        metrics.stream().map(m -> getArrayField(m.getField()))
            .filter(f -> f != null && f.length == 2 && !f[1].equalsIgnoreCase("size")).map(f -> f[0]),
        buckets.stream().map(b -> getArrayField(b.getField()))
            .filter(f -> f != null && f.length == 2 && !f[1].equalsIgnoreCase("size")).map(f -> f[0])),
        filters.stream().map(f -> getArrayField(f.getField()))
            .filter(f -> f != null && f.length == 2 && !f[1].equalsIgnoreCase("size")).map(f -> f[0])
    ).distinct().collect(Collectors.toList());
  }

  public static String parseMetric(String engine, String method, String field) {
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

  private static String parseField(String engine, String field) {
    String[] arrayField = getArrayField(field);
    String realField = field;
    if (arrayField != null) {
      if (arrayField.length == 2 && arrayField[1].equalsIgnoreCase("size")) {
        switch (engine) {
          case Config.SQL_ENGINE_PRESTO:
            realField = String.format("cardinality(%s)", arrayField[0]);
            break;
          case Config.SQL_ENGINE_SPARK_SQL:
            realField = String.format("size(%s)", arrayField[0]);
            break;
        }
      } else {
        return "f_" + field;
      }
    }
    return realField;
  }

  public static String parseHavingField(String engine, String field) {
    int i = field.indexOf(":");
    return parseMetric(engine, field.substring(0, i), field.substring(i + 1, field.length()));
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
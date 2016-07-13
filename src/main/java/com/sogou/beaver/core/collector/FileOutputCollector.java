package com.sogou.beaver.core.collector;

import com.sogou.beaver.Config;
import com.sogou.beaver.common.CommonUtils;
import com.sogou.beaver.core.meta.ColumnMeta;
import com.sogou.beaver.core.plan.CompoundQuery;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.*;

import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Tao Li on 2016/6/3.
 */
public class FileOutputCollector implements RelationOutputCollector {
  private final static String FILE_SEPARATOR = File.separator;
  private final static String FIELD_SEPARATOR = "\001";
  private final static String RECORD_SEPARATOR = "\n";
  private final static Charset CHARSET = StandardCharsets.UTF_8;
  private static String DATA_FILE_POSTFIX = ".data";
  private static String SCHEMA_FILE_POSTFIX = ".schema";

  private long jobId;
  private BufferedWriter dataFileWriter;
  private boolean isFirstLine = true;

  private static Path getOutputDir(long jobId) {
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        Config.FILE_OUTPUT_COLLECTOR_ROOT_DIR, String.valueOf(jobId)));
  }

  private static Path getDataFile(long jobId) {
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        Config.FILE_OUTPUT_COLLECTOR_ROOT_DIR, String.valueOf(jobId),
        String.valueOf(jobId) + DATA_FILE_POSTFIX));
  }

  private static Path getSchemaFile(long jobId) {
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        Config.FILE_OUTPUT_COLLECTOR_ROOT_DIR, String.valueOf(jobId),
        String.valueOf(jobId) + SCHEMA_FILE_POSTFIX));
  }

  public FileOutputCollector(long jobId) throws IOException {
    this.jobId = jobId;

    Path outputDir = getOutputDir(jobId);
    Files.deleteIfExists(outputDir);
    Files.createDirectory(outputDir);

    Path dataFile = getDataFile(jobId);
    Files.createFile(dataFile);
    dataFileWriter = Files.newBufferedWriter(dataFile, CHARSET, StandardOpenOption.WRITE);
  }

  @Override
  public void initColumnMetas(List<ColumnMeta> columnMetadatas) throws IOException {
    String header = columnMetadatas.stream()
        .map(meta -> meta.getColumnName()).collect(Collectors.joining(FIELD_SEPARATOR));

    Path schemaFile = getSchemaFile(jobId);
    Files.createFile(schemaFile);
    BufferedWriter schemaFileWriter = Files.newBufferedWriter(
        schemaFile, CHARSET, StandardOpenOption.WRITE);
    schemaFileWriter.write(header);
    schemaFileWriter.close();
  }

  @Override
  public void collect(List<String> values) throws IOException {
    String line = values.stream().map(value -> value.replace(FIELD_SEPARATOR, " "))
        .collect(Collectors.joining(FIELD_SEPARATOR));
    if (isFirstLine) {
      dataFileWriter.write(line);
      isFirstLine = false;
    } else {
      dataFileWriter.write(RECORD_SEPARATOR + line);
    }
  }

  @Override
  public void close() throws IOException {
    dataFileWriter.close();
  }

  public static JobResult getJobResult(long jobId, int start, int length) throws IOException {
    JobResult result = new JobResult();

    try (BufferedReader schemaFileReader = Files.newBufferedReader(getSchemaFile(jobId))) {
      String headers = schemaFileReader.readLine();
      if (headers != null) {
        result.setHeaders(getHeaderDescriptions(jobId, headers));
      }
    }

    try (BufferedReader dataFileReader = Files.newBufferedReader(getDataFile(jobId))) {
      List<String[]> values = new ArrayList<>();
      String line;
      long n = 0;
      while ((line = dataFileReader.readLine()) != null) {
        if (n >= start + length) {
          break;
        } else {
          if (n >= start) {
            values.add(line.split(FIELD_SEPARATOR));
          }
        }
        n++;
      }
      result.setValues(values);
    }

    return result;
  }

  public static StreamingOutput getStreamingOutput(long jobId) {
    return outputStream -> {
      try (BufferedReader schemaFileReader = Files.newBufferedReader(getSchemaFile(jobId))) {
        String headers = schemaFileReader.readLine();
        if (headers != null) {
          String record = CommonUtils.formatCSVRecord(getHeaderDescriptions(jobId, headers));
          outputStream.write((record + RECORD_SEPARATOR).getBytes("GBK"));
        }
      }

      try (BufferedReader reader = Files.newBufferedReader(getDataFile(jobId))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String record = CommonUtils.formatCSVRecord(line.split(FIELD_SEPARATOR));
          outputStream.write((record + RECORD_SEPARATOR).getBytes("GBK"));
        }
      }

      outputStream.flush();
    };
  }

  private static String[] getHeaderDescriptions(long jobId, String headers) {
    String[] headerDescriptions = headers.split(FIELD_SEPARATOR);
    try {
      Job job = Config.JOB_DAO.getJobById(jobId);
      if (job.getQueryType().equalsIgnoreCase(Config.QUERY_TYPE_COMPOUND)) {
        CompoundQuery query = CommonUtils.fromJson(job.getQueryPlan(), CompoundQuery.class);
        TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoByName(query.getTableName());
        Map<String, FieldInfo> fieldInfos =
            Config.FIELD_INFO_DAO.getFieldInfosByViewId(tableInfo.getId())
                .stream().collect(Collectors.toMap(f -> f.getName().replace(".", "_"), Function.identity()));
        Map<String, TableMetric> tableMetrics =
            Config.TABLE_METRIC_DAO.getTableMetricsByViewId(tableInfo.getId())
                .stream().collect(Collectors.toMap(TableMetric::getName, Function.identity()));
        Map<String, MethodInfo.MetricMethod> metricMethods =
            Config.METHOD_INFO_DAO.getMethodInfo().getMetricMethods()
                .stream().collect(Collectors.toMap(MethodInfo.MetricMethod::getName, Function.identity()));
        headerDescriptions = Stream.of(headers.split(FIELD_SEPARATOR))
            .map(header -> getHeaderDescription(header, fieldInfos, tableMetrics, metricMethods))
            .toArray(String[]::new);
      }
    } catch (ConnectionPoolException | SQLException | IOException e) {
      // ignore
    }
    return headerDescriptions;
  }

  private static String getHeaderDescription(String header, Map<String, FieldInfo> fieldInfos,
                                             Map<String, TableMetric> tableMetrics,
                                             Map<String, MethodInfo.MetricMethod> metricMethods) {
    if (tableMetrics.containsKey(header)) {
      return tableMetrics.get(header).getDescription();
    }
    if (fieldInfos.containsKey(header)) {
      return fieldInfos.get(header).getDescription();
    }
    for (MethodInfo.MetricMethod metricMethod : metricMethods.values()) {
      if (header.startsWith(metricMethod.getName() + "_")) {
        String field = header.substring(metricMethod.getName().length() + 1);
        if (fieldInfos.containsKey(field)) {
          return String.format("%s(%s)",
              metricMethod.getDescription(), fieldInfos.get(field).getDescription());
        }
      }
    }
    return header;
  }
}

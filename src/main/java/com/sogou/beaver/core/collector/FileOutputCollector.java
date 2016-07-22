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
    String fileName = jobId + DATA_FILE_POSTFIX;
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        Config.FILE_OUTPUT_COLLECTOR_ROOT_DIR, String.valueOf(jobId), fileName));
  }

  private static Path getSortDataFile(long jobId, int fieldId, boolean isDesc) {
    String fileName = jobId + ".data.sort." + fieldId + "." + (isDesc ? "desc" : "asc");
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        Config.FILE_OUTPUT_COLLECTOR_ROOT_DIR, String.valueOf(jobId), fileName));
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
        .map(meta -> meta.getColumnName() + ":" + meta.getColumnTypeName())
        .collect(Collectors.joining(FIELD_SEPARATOR));

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

  public static JobResult getJobResult(long jobId, int start, int length,
                                       int fieldId, boolean isDesc) throws IOException {
    JobResult result = new JobResult();

    List<JobResult.HeaderInfo> headerInfos = getHeaderInfos(jobId);
    result.setHeaderInfos(headerInfos);

    Path file;
    if (fieldId == -1) {
      file = getDataFile(jobId);
    } else {
      file = getOrCreateSortDataFile(jobId, fieldId, headerInfos.get(fieldId).getType(), isDesc);
    }

    try (BufferedReader dataFileReader = Files.newBufferedReader(file)) {
      List<String[]> values = scan(dataFileReader, start, length);
      result.setValues(values);
    }

    return result;
  }

  private static List<String[]> scan(BufferedReader reader, int start, int length)
      throws IOException {
    List<String[]> values = new ArrayList<>();
    String line;
    long n = 0;
    while ((line = reader.readLine()) != null) {
      if (n >= start + length) {
        break;
      } else {
        if (n >= start) {
          values.add(line.split(FIELD_SEPARATOR));
        }
      }
      n++;
    }
    return values;
  }

  private static Path getOrCreateSortDataFile(long jobId, int fieldId, String type, boolean isDesc)
      throws IOException {
    Path file = getSortDataFile(jobId, fieldId, isDesc);
    if (Files.exists(file)) {
      return file;
    }

    // sort -t $'\001' -k 3 -n -r 1235.data -o xxx

    boolean isNumber = false;
    type = type.toLowerCase();
    if (type.equals("int") || type.equals("long") || type.equals("bigint")
        || type.equals("float") || type.equals("double")) {
      isNumber = true;
    }

    String dataFileName = String.format("data/%s/%s.data", jobId, jobId);
    String sortDataFileName = String.format("data/%s/%s.data.sort.%s.%s",
        jobId, jobId, fieldId, isDesc ? "desc" : "asc");
    
    String command = String.format("sort -t $'\\001' -k %s %s %s %s -o %s",
        fieldId + 1, isNumber ? "-n" : "", isDesc ? "-r" : "",
        dataFileName, sortDataFileName);
    try {
      System.out.println(CommonUtils.runProcess(command, null, null));
    } catch (IOException e) {
      // ignore
    }

    return file;
  }

  public static StreamingOutput getStreamingOutput(long jobId) {
    return outputStream -> {
      List<JobResult.HeaderInfo> headerInfos = getHeaderInfos(jobId);
      String[] descriptions = headerInfos.stream()
          .map(headerInfo -> headerInfo.getDescription()).toArray(String[]::new);
      String description = CommonUtils.formatCSVRecord(descriptions);
      outputStream.write((description + RECORD_SEPARATOR).getBytes("GBK"));

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

  private static List<JobResult.HeaderInfo> getHeaderInfos(long jobId) {
    List<JobResult.HeaderInfo> headerInfos = new ArrayList<>();
    String headerLine = null;
    try (BufferedReader schemaFileReader = Files.newBufferedReader(getSchemaFile(jobId))) {
      headerLine = schemaFileReader.readLine();
    } catch (IOException e) {
      // ignore
    }
    if (headerLine == null) {
      return headerInfos;
    }

    String[] headers = headerLine.split(FIELD_SEPARATOR);
    for (int i = 0; i < headers.length; i++) {
      String[] arr = headers[i].split(":");
      headerInfos.add(new JobResult.HeaderInfo(i, arr[0], arr[0],
          arr.length == 2 ? arr[1] : "string"));
    }

    try {
      Job job = Config.JOB_DAO.getJobById(jobId);
      if (job.getQueryType().equalsIgnoreCase(Config.QUERY_TYPE_COMPOUND)) {
        CompoundQuery query = CommonUtils.fromJson(job.getQueryPlan(), CompoundQuery.class);
        TableInfo tableInfo = Config.TABLE_INFO_DAO.getTableInfoByName(query.getTableName());
        Map<String, FieldInfo> fieldInfos =
            Config.FIELD_INFO_DAO.getFieldInfosByViewId(tableInfo.getId()).stream()
                .collect(Collectors.toMap(f -> f.getName().replace(".", "_"), Function.identity()));
        Map<String, TableMetric> tableMetrics =
            Config.TABLE_METRIC_DAO.getTableMetricsByViewId(tableInfo.getId()).stream()
                .collect(Collectors.toMap(TableMetric::getName, Function.identity()));
        Map<String, MethodInfo.MetricMethod> metricMethods =
            Config.METHOD_INFO_DAO.getMethodInfo().getMetricMethods().stream()
                .collect(Collectors.toMap(MethodInfo.MetricMethod::getName, Function.identity()));

        headerInfos.stream().forEach(headerInfo -> {
          if (tableMetrics.containsKey(headerInfo.getName())) {
            headerInfo.setDescription(tableMetrics.get(headerInfo.getName()).getDescription());
          }
          if (fieldInfos.containsKey(headerInfo.getName())) {
            headerInfo.setDescription(fieldInfos.get(headerInfo.getName()).getDescription());
          }
          metricMethods.values().stream().forEach(metricMethod -> {
            if (headerInfo.getName().startsWith(metricMethod.getName() + "_")) {
              String field = headerInfo.getName().substring(metricMethod.getName().length() + 1);
              if (fieldInfos.containsKey(field)) {
                headerInfo.setDescription(String.format("%s(%s)",
                    metricMethod.getDescription(), fieldInfos.get(field).getDescription()));
              }
            }
          });
        });
      }
    } catch (ConnectionPoolException | SQLException | IOException e) {
      // ignore
    }

    return headerInfos;
  }
}

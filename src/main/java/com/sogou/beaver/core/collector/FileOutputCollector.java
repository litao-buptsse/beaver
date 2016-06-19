package com.sogou.beaver.core.collector;

import com.sogou.beaver.core.meta.ColumnMeta;
import com.sogou.beaver.model.JobResult;
import com.sogou.beaver.util.CommonUtils;

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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Tao Li on 2016/6/3.
 */
public class FileOutputCollector implements RelationOutputCollector {
  private final static String FILE_SEPARATOR = File.separator;
  private final static String FIELD_SEPARATOR = "\001";
  private final static String RECORD_SEPARATOR = "\n";
  private final static Charset CHARSET = StandardCharsets.UTF_8;
  private static String OUTPUT_ROOT_DIR = "data";
  private static String DATA_FILE_POSTFIX = ".data";
  private static String SCHEMA_FILE_POSTFIX = ".schema";

  private long jobId;
  private BufferedWriter dataFileWriter;
  private boolean isFirstLine = true;

  private static Path getOutputDir(long jobId) {
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        OUTPUT_ROOT_DIR, String.valueOf(jobId)));
  }

  private static Path getDataFile(long jobId) {
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        OUTPUT_ROOT_DIR, String.valueOf(jobId),
        String.valueOf(jobId) + DATA_FILE_POSTFIX));
  }

  private static Path getSchemaFile(long jobId) {
    return Paths.get(CommonUtils.formatPath(FILE_SEPARATOR,
        OUTPUT_ROOT_DIR, String.valueOf(jobId),
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
    String line = values.stream().collect(Collectors.joining(FIELD_SEPARATOR));
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

  public static void setOutputRootDir(String outputRootDir) {
    FileOutputCollector.OUTPUT_ROOT_DIR = outputRootDir;
  }

  public static String getOutputRootDir() {
    return FileOutputCollector.OUTPUT_ROOT_DIR;
  }

  public static JobResult getJobResult(long jobId, int start, int length) throws IOException {
    JobResult result = new JobResult();

    try (BufferedReader schemaFileReader = Files.newBufferedReader(getSchemaFile(jobId))) {
      String headers = schemaFileReader.readLine();
      if (headers != null) {
        result.setHeaders(headers.split(FIELD_SEPARATOR));
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
          String record = CommonUtils.formatCSVRecord(headers.split(FIELD_SEPARATOR));
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
}

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
import java.util.stream.Stream;

/**
 * Created by Tao Li on 2016/6/3.
 */
public class FileOutputCollector implements RelationOutputCollector {
  private final static String FILE_SEPARATOR = File.separator;
  private final static String FIELD_SEPARATOR = "\001";
  private final static String RECORD_SEPARATOR = "\n";
  private final static Charset CHARSET = StandardCharsets.UTF_8;
  private static String outputRootDir = "data";
  private BufferedWriter writer;

  public FileOutputCollector(String fileName) throws IOException {
    Path rootDir = Paths.get(outputRootDir);
    if (!Files.exists(rootDir)) {
      Files.createDirectory(rootDir);
    }
    Path file = Paths.get(rootDir + FILE_SEPARATOR + fileName);
    Files.createFile(file);
    writer = Files.newBufferedWriter(file, CHARSET, StandardOpenOption.WRITE);
  }

  @Override
  public void initColumnMetas(List<ColumnMeta> columnMetadatas) throws IOException {
    String header = columnMetadatas.stream()
        .map(meta -> meta.getColumnName()).collect(Collectors.joining(FIELD_SEPARATOR));
    writer.write(header);
  }

  @Override
  public void collect(List<String> values) throws IOException {
    String line = RECORD_SEPARATOR + values.stream().collect(Collectors.joining(FIELD_SEPARATOR));
    writer.write(line);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static void setOutputRootDir(String outputRootDir) {
    FileOutputCollector.outputRootDir = outputRootDir;
  }

  public static String getOutputRootDir() {
    return FileOutputCollector.outputRootDir;
  }

  public static JobResult getJobResult(String file, int start, int length) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(file))) {
      JobResult result = new JobResult();

      String headers = reader.readLine();
      if (headers != null) {
        result.setHeaders(headers.split(FIELD_SEPARATOR));
      }

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
      result.setValues(values);

      return result;
    }
  }

  public static StreamingOutput getStreamingOutput(String file) {
    return outputStream -> {
      try (BufferedReader reader = Files.newBufferedReader(Paths.get(file))) {
        String line;
        while ((line = reader.readLine()) != null) {
          outputStream.write((Stream.of(line.split(FIELD_SEPARATOR))
              .map(value -> CommonUtils.formatCSVValue(value))
              .collect(Collectors.joining(",")) + RECORD_SEPARATOR).getBytes("GBK"));
        }
        outputStream.flush();
      }
    };
  }
}

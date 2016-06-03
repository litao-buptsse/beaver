package com.sogou.beaver.core.engine;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Tao Li on 2016/6/3.
 */
public class FileOutputCollector implements Closeable {
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

  public void collect(List<String> values) throws IOException {
    String line = values.stream().collect(Collectors.joining(FIELD_SEPARATOR)) + RECORD_SEPARATOR;
    writer.write(line);
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static void initOutputRootDir(String outputRootDir) {
    FileOutputCollector.outputRootDir = outputRootDir;
  }
}

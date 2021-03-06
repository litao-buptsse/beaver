package com.sogou.beaver.core.engine;

import com.sogou.beaver.core.collector.OutputCollector;
import com.sogou.beaver.core.collector.RelationOutputCollector;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Tao Li on 2016/6/6.
 */
public abstract class AbstractSQLEngine implements SQLEngine {
  @Override
  public boolean execute(String sql, Map<String, String> info) throws EngineExecutionException {
    try (RelationOutputCollector collector = getRelationOutputCollector()) {
      return doExecute(sql, info, collector);
    } catch (IOException e) {
      throw new EngineExecutionException("Failed to init output collector", e);
    }
  }

  public abstract boolean doExecute(String sql, Map<String, String> info,
                                    RelationOutputCollector collector)
      throws EngineExecutionException;

  public abstract RelationOutputCollector getRelationOutputCollector() throws IOException;

  public OutputCollector getOutputCollector() throws IOException {
    return getRelationOutputCollector();
  }
}

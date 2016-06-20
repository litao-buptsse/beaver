package com.sogou.beaver.core.engine;

import java.util.Map;

/**
 * Created by Tao Li on 2016/6/1.
 */
public interface SQLEngine {
  boolean execute(String sql, Map<String, String> info) throws EngineExecutionException;
}

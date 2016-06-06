package com.sogou.beaver.core.engine;

import java.sql.SQLException;

/**
 * Created by Tao Li on 2016/6/1.
 */
public interface SQLEngine {
  boolean execute(String sql) throws SQLException;
}

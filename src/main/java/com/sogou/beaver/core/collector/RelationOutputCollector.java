package com.sogou.beaver.core.collector;

import com.sogou.beaver.core.meta.ColumnMeta;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/6.
 */
public interface RelationOutputCollector extends OutputCollector {
  void initColumnMetas(List<ColumnMeta> columnMetadatas) throws IOException;
}

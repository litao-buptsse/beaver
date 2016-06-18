package com.sogou.beaver.core.engine;

/**
 * Created by Tao Li on 2016/3/21.
 */
public class EngineExecutionException extends Exception {
  public EngineExecutionException(String message) {
    super(message);
  }

  public EngineExecutionException(String message, Throwable cause) {
    super(message, cause);
  }

  public EngineExecutionException(Throwable cause) {
    super(cause);
  }
}

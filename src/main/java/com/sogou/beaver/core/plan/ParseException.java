package com.sogou.beaver.core.plan;

/**
 * Created by Tao Li on 2016/6/3.
 */
public class ParseException extends Exception {
  public ParseException(String message) {
    super(message);
  }

  public ParseException(String message, Throwable cause) {
    super(message, cause);
  }

  public ParseException(Throwable cause) {
    super(cause);
  }
}

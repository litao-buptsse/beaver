package com.sogou.beaver.util;

import com.fasterxml.jackson.databind.util.JSONPObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class CommonUtils {
  private final static Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

  private static String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  public static String now() {
    return LocalDateTime.now().format(DateTimeFormatter.ofPattern(TIME_FORMAT));
  }

  public static String ip() {
    String localip = null;// 本地IP，如果没有配置外网IP则返回它
    String netip = null;// 外网IP
    try {
      Enumeration<NetworkInterface> netInterfaces = NetworkInterface
          .getNetworkInterfaces();
      InetAddress ip = null;
      boolean finded = false;// 是否找到外网IP
      while (netInterfaces.hasMoreElements() && !finded) {
        NetworkInterface ni = netInterfaces.nextElement();
        Enumeration<InetAddress> address = ni.getInetAddresses();
        while (address.hasMoreElements()) {
          ip = address.nextElement();
          if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress()
              && !ip.getHostAddress().contains(":")) {// 外网IP
            netip = ip.getHostAddress();
            finded = true;
            break;
          } else if (ip.isSiteLocalAddress()
              && !ip.isLoopbackAddress()
              && !ip.getHostAddress().contains(":")) {// 内网IP
            localip = ip.getHostAddress();
          }
        }
      }
    } catch (Exception ex) {
      LOG.error("Get IP Failed");
    }
    if (netip != null && !"".equals(netip)) {
      return netip;
    } else {
      return localip;
    }
  }

  public static String formatSQLValue(String value) {
    return value == null ? null : String.format("'%s'", value.replace("'", "''"));
  }

  public static String formatCSVValue(String value) {
    String tmp = value.replace("\"", "\"\"");
    return tmp.contains(",") ? String.format("\"%s\"", tmp) : tmp;
  }

  public static String formatCSVRecord(String[] values) throws UnsupportedEncodingException {
    return Stream.of(values)
        .map(value -> CommonUtils.formatCSVValue(value))
        .collect(Collectors.joining(","));
  }

  public static Object formatJSONPObject(String callback, Object obj) {
    return callback != null ? new JSONPObject(callback, obj) : obj;
  }

  public static String formatPath(String fileSeperator, String... parts) {
    return Stream.of(parts).collect(Collectors.joining(fileSeperator));
  }
}

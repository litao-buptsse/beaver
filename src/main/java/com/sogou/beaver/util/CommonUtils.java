package com.sogou.beaver.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Enumeration;

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

  public static String formatSQLValue(String str) {
    return str == null ? null : String.format("'%s'", str.replace("'", "''"));
  }

  public static String readFile(String file, long page, long size, boolean withHeader)
      throws IOException {
    BufferedReader reader = Files.newBufferedReader(Paths.get(file));
    StringBuffer sb = new StringBuffer();
    if (withHeader) {
      String header = reader.readLine();
      sb.append(header + "\n");
    }
    String line;
    long n = 0;
    long start = (page - 1) * size;
    long end = page * size - 1;
    while ((line = reader.readLine()) != null) {
      if (n > end) {
        break;
      } else {
        if (n >= start) {
          sb.append(line + "\n");
        }
      }
      n++;
    }
    reader.close();
    return sb.toString();
  }
}

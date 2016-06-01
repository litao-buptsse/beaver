package com.sogou.beaver.util;

import com.sogou.beaver.db.ConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
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

  public static String getRealIp() {
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
}

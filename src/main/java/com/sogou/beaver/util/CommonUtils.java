package com.sogou.beaver.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

  public static long convertStringToTimestamp(String str, String format) {
    return LocalDateTime.parse(str, DateTimeFormatter.ofPattern(format))
        .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
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
    return formatSQLValue(value, true);
  }

  public static String formatSQLValue(String value, boolean withQuotes) {
    return value == null ? null :
        (withQuotes ? String.format("'%s'", value.replace("'", "''")) : value);
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

  public static <T> T fromJson(String json, Class<T> clazz) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), clazz);
  }

  public static String toJson(Object obj) throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(obj);
  }

  public static Response sendHttpRequest(String method, String uri, String responseType) {
    Client client = ClientBuilder.newClient(new ClientConfig()).
        property(ClientProperties.CONNECT_TIMEOUT, 1000).
        property(ClientProperties.READ_TIMEOUT, 1000);
    WebTarget target = client.target(uri);
    Invocation.Builder invocationBuilder = target.request(responseType);
    return invocationBuilder.method(method);
  }
}

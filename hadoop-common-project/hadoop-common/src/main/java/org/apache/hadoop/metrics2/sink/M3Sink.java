package org.apache.hadoop.metrics2.sink;

import com.uber.m3.client.CommonTag;
import com.uber.m3.client.MetricConfig;
import com.uber.m3.client.Scope;
import com.uber.m3.client.Scopes;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class M3Sink implements MetricsSink, AutoCloseable {
  private static final Log LOG = LogFactory.getLog(M3Sink.class);

  private static final String SERVICE_NAME_KEY = "service_name";
  private static final String SERVICE_ENV_KEY = "service_env";
  private static final String SERVER_HOST_KEY = "server_host";
  private static final String SERVER_PORT_KEY = "server_port";

  private static final String OP_TAG_NAME = "op";
  private static final String USER_TAG_NAME = "user";
  private static final String NNTOP_TOTAL = "nntop_total";
  private static final String NNTOP_USER = "nntop_user";

  // in the form of: getfileinfo.TotalCount
  private static final Pattern TOTAL_COUNT_REGEX =
      Pattern.compile("op=(.*)\\.TotalCount");
  // in the form of: getfileinfo.user=<username>.count
  private static final Pattern USER_COUNT_REGEX =
      Pattern.compile("op=(.*)\\.user=(.*)\\.count");

  private Scope scope;

  @Override
  public void init(SubsetConfiguration conf) {
    final String serviceName = conf.getString(SERVICE_NAME_KEY);
    final String serviceEnv = conf.getString(SERVICE_ENV_KEY);
    final String serverHost = conf.getString(SERVER_HOST_KEY);
    final int serverPort = Integer.parseInt(conf.getString(SERVER_PORT_KEY));

    LOG.info("Initializing M3 metrics sink");
    LOG.info("serviceName: " + serviceName);
    LOG.info("serviceEnv: " + serviceEnv);
    LOG.info("serverHost: " + serverHost);
    LOG.info("serverPort: " + serverPort);

    Map<String, String> commonTags = Scopes.getCommonTags(
        new CommonTag.Service(serviceName),
        new CommonTag.Environment(serviceEnv));

    scope = Scopes.getScopeForConfig(
        MetricConfig.builder()
            .setCommonTags(commonTags)
            .setHost(serverHost)
            .setPort(serverPort)
            .build());
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    Map<String, String> tags = new HashMap<>();
    for (MetricsTag tag : record.tags()) {
      if (tag.value() != null) {
        tags.put(tag.name(), tag.value());
      }
    }

    for (AbstractMetric metric : record.metrics()) {
      switch (metric.type()) {
        case GAUGE:
          scope.gauge(metric.name(), metric.value().longValue(), tags);
          LOG.debug("Emitted gauge metrics " + metric.name() + " with value "
              + metric.value().longValue());
          break;
        case COUNTER:
          String metricsName = metric.name();
          Map<String, String> newTags = tags;

          // special handling of nntop metrics
          if (metricsName.startsWith("op=")) {
            newTags = new HashMap<>(tags);
            Matcher m1 = TOTAL_COUNT_REGEX.matcher(metricsName);
            if (m1.matches()) {
              String opName = m1.group(1).replace("*", "all");
              newTags.put(OP_TAG_NAME, opName);
              metricsName = NNTOP_TOTAL;
            } else {
              Matcher m2 = USER_COUNT_REGEX.matcher(metricsName);
              if (m2.matches()) {
                String opName = m2.group(1).replace("*", "all");
                String ugi = m2.group(2);
                newTags.put(OP_TAG_NAME, opName);
                newTags.put(USER_TAG_NAME, ugi);
                metricsName = NNTOP_USER;
              }
            }
          }
          scope.count(metricsName, metric.value().longValue(), newTags);
          LOG.debug("Emitted counter metrics " + metricsName + " with value "
              + metric.value().longValue());
          break;
      }
    }
  }

  @Override
  public void flush() {
    // Do nothing
  }

  @Override
  public void close() throws Exception {
    scope.close();
  }
}

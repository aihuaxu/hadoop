package org.apache.hadoop.hdfs.util;


import com.google.common.cache.*;
import com.uber.m3.tally.*;
import com.uber.m3.tally.m3.M3Reporter;
import com.uber.m3.util.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * MetricsPublisher is thread safe.
 */
public class MetricsPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsPublisher.class);
  private static volatile MetricsPublisher INSTANCE;

  private static final String SERVICE_NAME = "hadoop";
  private static final String DATANODE_TAG = "datanode";

  private final int metricsSamplePercent;
  private Scope scope;

  private long lastMetricLogTime;

  /**
   * HDFS client emits metrics with tag "datanode". As there are thousands
   * of datanodes and tagging the scope actually creates a new scope which
   * is rather expensive, we cache the subscopes.
   */
  private LoadingCache<String, Scope> dnSubscopeCache;

  /**
   * @param metricsReporterAddr In the form of "hostname:port"
   */
  public static MetricsPublisher getInstance(int metricsSamplePercent,
                                             String metricsReporterAddr) {
    if (INSTANCE == null) {
      synchronized (MetricsPublisher.class) {
        if (INSTANCE == null) {
          INSTANCE = new MetricsPublisher(metricsSamplePercent, metricsReporterAddr);
        }
      }
    }
    return INSTANCE;
  }

  private MetricsPublisher(int metricsSamplePercent, String metricsReporterAddr) {
    this.metricsSamplePercent = metricsSamplePercent;

    try {
      scope = createM3Client(metricsReporterAddr);
    } catch (Exception e) {
      LOG.error("Unable to initialize m3 client.", e);
    }
    dnSubscopeCache = CacheBuilder.newBuilder()
        .maximumSize(5000)
        .expireAfterAccess(5, TimeUnit.MINUTES)
        .removalListener(new RemovalListener<String, Scope>() {
          @Override
          public void onRemoval(RemovalNotification<String, Scope> ntc) {
            try {
              ntc.getValue().close();
            } catch (ScopeCloseException e) {
              LOG.warn("failed to close scope", e);
            }
          }
        })
        .build(new CacheLoader<String, Scope>() {
          @Override
          public Scope load(String datanode) {
            try {
              Map<String, String> map = new HashMap<>();
              map.put(DATANODE_TAG, datanode);
              return scope.tagged(map);
            } catch (Exception e) {
              LOG.error("failed to create subscope", e);
              return null;
            }
          }
        });
  }

  public boolean shallIEmit() {
    if(scope == null) {
      LOG.error("Scope is null");
    }

    return scope != null
        && ThreadLocalRandom.current().nextInt(100) < metricsSamplePercent;
  }

  /**
   * For client side datanode metrics which adds a special tag "datanode".
   */
  public void emit(MetricType metricType, String datanode,
                            String name, long amount) {
    if(scope == null) {
      LOG.error("Scope is null");
    }

    if (scope != null && datanode != null && datanode.length() != 0) {
      try {
        if (System.currentTimeMillis() - lastMetricLogTime > 60 * 1000) { // 60 seconds
          LOG.info("emitting metrics...");
          lastMetricLogTime = System.currentTimeMillis();
        }

        Scope dnScope = dnSubscopeCache.get(datanode);
        switch (metricType) {
          case GAUGE:
            dnScope.gauge(name).update(amount);
            break;
          case COUNTER:
            dnScope.counter(name).inc(amount);
            break;
        }
      } catch (ExecutionException x) {
        LOG.warn("Unable to emit metrics", x);
      }
    }
  }

  public enum MetricType {
    GAUGE,
    COUNTER
  }

  private static Scope createM3Client(String metricsReporterAddr) {
    ImmutableMap.Builder<String, String> tagsBuilder = new ImmutableMap.Builder<>();
    tagsBuilder.put(M3Reporter.SERVICE_TAG, SERVICE_NAME);

    String uberEnviroment = System.getenv().get("UBER_ENVIRONMENT");
    if (uberEnviroment == null || uberEnviroment.length() == 0) {
      uberEnviroment = M3Reporter.DEFAULT_TAG_VALUE;
    }
    tagsBuilder.put(M3Reporter.ENV_TAG, uberEnviroment);

    String[] splits = metricsReporterAddr.trim().split(":");
    String hostname = splits[0];
    int port = Integer.parseInt(splits[1]);
    StatsReporter reporter =
        new M3Reporter.Builder(new InetSocketAddress(hostname, port))
            // must set to false. otherwise total cardinality would be greater
            // than 100K which would cause an m3 ban
            .includeHost(false)
            .commonTags(tagsBuilder.build())
            .build();

    ScopeBuilder scopeBuilder = new RootScopeBuilder().reporter(reporter);
    return scopeBuilder.reportEvery(com.uber.m3.util.Duration.ofSeconds(10));
  }
}

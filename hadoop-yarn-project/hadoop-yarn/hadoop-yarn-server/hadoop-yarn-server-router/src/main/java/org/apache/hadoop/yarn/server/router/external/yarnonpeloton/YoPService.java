package org.apache.hadoop.yarn.server.router.external.yarnonpeloton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service has a monitoring thread which periodically calls Peloton
 * to get currently available hosts for YARN NMs and start NMs on these hosts
 */
public class YoPService extends AbstractService {

  private final static Logger LOG =
    LoggerFactory.getLogger(YoPService.class);

  private volatile boolean stopped = false;
  private MonitoringThread monitoringThread;
  private PelotonHelper pelotonHelper;

  /**
   * Construct the service.
   */
  public YoPService() {
    super(YoPService.class.getName());
    this.monitoringThread = new MonitoringThread();
    pelotonHelper = new PelotonHelper();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // TODO:
    // add YARN configurations for Peloton cluster, zk, etc

    // initialize Peloton service client connection, could be done within MonitoringThread too
    pelotonHelper.initialize();
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    this.monitoringThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    stopped = true;
    this.monitoringThread.interrupt();
    try {
      this.monitoringThread.join();
    } catch (InterruptedException e) {
    }
    super.serviceStop();
  }

  private class MonitoringThread extends Thread {
    public MonitoringThread() {
      super("YoP Service Monitor");
    }

    @Override
    public void run() {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        // TODO
        // get available pools & hosts from Peloton
        // start job instances for NM

        // placeholder for the monitoring thread
        LOG.info("Monitoring thread is running");
        // validate Peloton connection
        pelotonHelper.lookupPool();

        try {
          Thread.sleep(1000*60*5);
        } catch (InterruptedException e) {
          LOG.error("Monitoring thread exception", e);
        }
      }
    }
  }
}

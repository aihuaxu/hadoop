package org.apache.hadoop.yarn.server.router.external.peloton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.router.clientrm.RouterClientRMService;
import org.apache.hadoop.yarn.server.router.store.RouterStateStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.ROUTER_KEYTAB_FILE_KEY;

/**
 * This service has a monitoring thread which periodically calls Peloton
 * to get currently available hosts for YARN NMs and start NMs on these hosts
 */
public class YoPService extends AbstractService {

  private final static Logger LOG =
    LoggerFactory.getLogger(YoPService.class);

  private RouterStateStoreService routerStateStore;
  private RouterClientRMService clientRMService;
  private ResourceManagerAdministrationProtocol rmAdminProxy;
  private volatile boolean stopped = false;
  private MonitoringThread monitoringThread;
  /**
   * Construct the service.
   */
  public YoPService(RouterStateStoreService routerStateStore, RouterClientRMService clientRMService) {
    super(YoPService.class.getName());
    this.routerStateStore = routerStateStore;
    this.clientRMService = clientRMService;
    this.monitoringThread = new MonitoringThread();
  }

  @Override
  protected void serviceInit(final Configuration conf) throws Exception {
    try {
      UserGroupInformation.setConfiguration(conf);
      SecurityUtil.login(conf, ROUTER_KEYTAB_FILE_KEY,
          ROUTER_KERBEROS_PRINCIPAL_KEY);
      UserGroupInformation user = UserGroupInformation.getLoginUser();
      rmAdminProxy = user.doAs(
          new PrivilegedExceptionAction<ResourceManagerAdministrationProtocol>() {
            @Override
            public ResourceManagerAdministrationProtocol run()
                throws Exception {
              return ClientRMProxy.createRMProxy(conf,
                  ResourceManagerAdministrationProtocol.class);
            }
          });
    } catch (IOException e) {
      String message = "Error while creating Router RMAdmin Service for login user";
      LOG.info(message);
      throw new YarnRuntimeException(message, e);
    } catch (Exception e) {
      throw new YarnRuntimeException(e);
    }

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
    private PelotonHelper pelotonHelper;

    public MonitoringThread() {
      super("YoP Service Monitor");
    }

    @Override
    public void run() {
      pelotonHelper = new PelotonHelper(routerStateStore, clientRMService, rmAdminProxy);
      // initialize Peloton client connection
      LOG.info("Initializing Peloton service clients");
      pelotonHelper.initialize(getConfig());

      while (!stopped) {
        try {
          LOG.info("YARN on Peloton monitoring thread is running");
          pelotonHelper.connectPelotonServices();
          LOG.info("set reclaimer hosts....");
          pelotonHelper.setReclaimerHosts();
          LOG.info("Start NMs on Peloton....");
          pelotonHelper.startNMsOnPeloton();
        } catch (Exception e) {
          LOG.error("Yarn on Peloton monitoring thread exception", e);
        }

        if (!stopped) {
          try {
            Thread.sleep(1000 * 60 * 1);
          } catch (InterruptedException e) {
            if (stopped) {
              LOG.info("Yarn on Peloton monitoring thread interrupted from serviceStop", e);
              break;
            } else {
              LOG.error("Yarn on Peloton monitoring thread is interrupted but not from serviceStop",
                e);
            }
          }
        }
      }
    }
  }
}

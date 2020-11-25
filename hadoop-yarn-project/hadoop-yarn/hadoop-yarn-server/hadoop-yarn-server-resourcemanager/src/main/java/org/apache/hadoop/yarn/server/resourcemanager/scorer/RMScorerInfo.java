package org.apache.hadoop.yarn.server.resourcemanager.scorer;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.util.MBeans;

/**
 * JMX bean listing scores of node managers in Scorer Service.
 */

public class RMScorerInfo implements RMScorerInfoBeans {
  private static final Log LOG = LogFactory.getLog(RMScorerInfo.class);
  private ScorerService scorerService;

  /**
   * Constructor for RMScorerInfo registers the bean with JMX.
   *
   * @param scorerService the Scorer service instance in active RM
   */
  public RMScorerInfo(ScorerService scorerService) {
    this.scorerService = scorerService;
    try {
      StandardMBean bean;
      bean = new StandardMBean(this, RMScorerInfoBeans.class);
      MBeans.register("ResourceManager", "RMScorerInfo", bean);
    } catch (NotCompliantMBeanException e) {
      LOG.warn("Error registering RMScorerInfo MBean", e);
    }
    LOG.info("Registered RMScorerInfo MBean");
  }

  /**
   * Implements getNodeManagerScores()
   *
   * @return JSON formatted string containing scores of external node managers in Scorer service
   */
  @Override
  public String getNodeManagerScores() {
    return scorerService.getHostScores();
  }
}

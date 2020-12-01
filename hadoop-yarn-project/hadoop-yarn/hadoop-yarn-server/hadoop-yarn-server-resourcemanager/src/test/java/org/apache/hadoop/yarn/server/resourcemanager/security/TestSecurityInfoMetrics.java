package org.apache.hadoop.yarn.server.resourcemanager.security;

import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

public class TestSecurityInfoMetrics extends TestCase {

    @Test
    public void testSetSizeDelegationTokenCancel(){
        SecurityInfoMetrics securityInfoMetrics = SecurityInfoMetrics.getMetrics();
        securityInfoMetrics.setSizeDelegationTokenCancel(1);
        Assert.assertEquals(1, securityInfoMetrics.sizeDelegationTokenCancel.value());
        securityInfoMetrics.setSizeDelegationTokenCancel(2);
        Assert.assertEquals(2, securityInfoMetrics.sizeDelegationTokenCancel.value());
    }
}
/**
 * Copyright (c) 2017 Netflix, Inc.  All rights reserved.
 */
package com.netflix.ndbench.plugin.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.inject.ImplementedBy;

/**
 * @author vchella
 */
@ImplementedBy(CassJavaDriverManagerImpl.class)
public interface CassJavaDriverManager {
    Cluster registerCluster(String clName, String contactPoint, int connections, int port, boolean allowBetaProtocol, String username, String password);
    Cluster registerCluster(String clName, String contactPoint, int connections, int port);
    Session getSession(Cluster cluster);

    void shutDown();
}

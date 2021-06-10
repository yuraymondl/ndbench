package com.netflix.ndbench.plugin.datastax4;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import java.util.List;

public class CassJavaDriverManagerImpl implements CassJavaDriverManager {

    @Override public CqlSession getSession(String sessionName, String contactPoint, int connections, int port) {
        return getSession(sessionName, contactPoint, connections, port,  null, null);
    }

    @Override
    public CqlSession getSession(
            String sessionName, String contactPoint, int connections, int port, String username,
            String password) {
        ProgrammaticDriverConfigLoaderBuilder configLoader = DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.SESSION_NAME, sessionName)
                .withString(DefaultDriverOption.CONTACT_POINTS, contactPoint + ":" + port)
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, connections)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, connections)
                .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 32768);
        if (username != null && password != null) {
            configLoader.withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, username)
                    .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, password);
        }

        return CqlSession.builder().withConfigLoader(configLoader.build()).build();
    }

    @Override public CqlSession getSession(String sessionName, List<String> contactPoints, int connections, int port) {
        return getSession(sessionName, contactPoints, connections, port,  null, null);
    }

    @Override
    public CqlSession getSession(
            String sessionName, List<String> contactPoints, int connections, int port, String username,
            String password) {
        ProgrammaticDriverConfigLoaderBuilder configLoader = DriverConfigLoader.programmaticBuilder()
                .withString(DefaultDriverOption.SESSION_NAME, sessionName)
                .withStringList(DefaultDriverOption.CONTACT_POINTS, contactPoints)
	        .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, "us-west2") //hardcoded for now
	        .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4") //hardcoded for now
                .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, connections)
                .withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, connections)
                .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 32768);
        if (username != null && password != null) {
            configLoader.withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, username)
                    .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, password)
	            .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class);
        }

        return CqlSession.builder().withConfigLoader(configLoader.build()).build();
    }

}

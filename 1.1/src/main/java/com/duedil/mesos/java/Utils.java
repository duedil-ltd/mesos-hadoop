package com.duedil.mesos.java;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

public class Utils {

    private static final String SCHEDULER_API_PATH = "/api/v1/scheduler";
    private static final String EXECUTOR_API_PATH = "/api/v1/executor";
    private static final String VERSION_API_PATH = "/version";

    public static URI schedulerEndpoint(URI masterUri) {
        return URI.create(masterUri.toString() + SCHEDULER_API_PATH);
    }

    public static URI executorEndpoint(String agentEndpoint) {
        checkNotNull(agentEndpoint);
        return URI.create(agentEndpoint + EXECUTOR_API_PATH);
    }

    public static URI versionEndpoint(URI masterUri) {
        return URI.create(masterUri.toString() + VERSION_API_PATH);
    }

    public static String getEnv(String envVar) {
        String result = System.getenv(envVar);

        if (result == null) {
            throw new NullPointerException("Non-existent environment variable: " + envVar);
        }

        return result;
    }
}

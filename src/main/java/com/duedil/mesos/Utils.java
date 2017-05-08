package com.duedil.mesos;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public static String getEnvOrThrow(String envVar) {
        String result = System.getenv(envVar);

        if (result == null) {
            throw new NullPointerException("Non-existent environment variable: " + envVar);
        }

        return result;
    }

    /**
     * This normalises all the times to milliseconds.
     */
    public static class TimeConversion {

        private static final TimeConversion INSTANCE = new TimeConversion();
        private static final Pattern TIME_RE = Pattern.compile("^(\\d+)(\\w+)$");

        public static TimeConversion getInstance() {
            return INSTANCE;
        }

        public long parseToMillis(final String input) {
            checkNotNull(input);
            Matcher m = TIME_RE.matcher(input);
            if (m.matches()) {
                int duration = Integer.valueOf(m.group(1));
                String unit = m.group(2);

                return duration * Conversion.fromString(unit).milliseconds;
            }

            throw new IllegalArgumentException();
        }

        public final static long MILLIS_IN_MILLISECOND = 1;
        public final static long MILLIS_IN_SECOND = MILLIS_IN_MILLISECOND * 1000;
        public final static long MILLIS_IN_MINUTE = MILLIS_IN_SECOND * 60;
        public final static long MILLIS_IN_HOUR = MILLIS_IN_MINUTE * 60;
        public final static long MILLIS_IN_DAY = MILLIS_IN_HOUR * 24;
        public final static long MILLIS_IN_WEEK = MILLIS_IN_DAY * 7;

        private enum Conversion {
            MS(MILLIS_IN_MILLISECOND),
            SECS(MILLIS_IN_SECOND),
            MINS(MILLIS_IN_MINUTE),
            HRS(MILLIS_IN_HOUR),
            DAYS(MILLIS_IN_DAY),
            WEEKS(MILLIS_IN_WEEK);

            private final long milliseconds;

            Conversion(final long milliseconds) {
                this.milliseconds = milliseconds;
            }

            private static final Map<String, Conversion> stringToEnum = new HashMap<>();
            static {
                for (Conversion conversion : values()) {
                    stringToEnum.put(conversion.toString(), conversion);
                }
            }

            public static Conversion fromString(String durationString) {
                return stringToEnum.get(durationString.toUpperCase());
            }

        }

        private TimeConversion() {}
    }

}

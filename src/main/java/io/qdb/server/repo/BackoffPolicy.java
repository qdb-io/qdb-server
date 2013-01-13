package io.qdb.server.repo;

/**
 * Calculates delay intervals base on consecutive failure counts.
 */
public interface BackoffPolicy {

    /**
     * Convert a failure count into a the delay in ms.
     */
    int getDelayMs(int failureCount);

    /**
     * What is the default max delay time?
     */
    int getMaxDelayMs();

    /**
     * Sleep for the appropriate amount of time. Ignores InterruptedException from Thread.sleep.
     */
    void sleep(int failureCount);

    /**
     * Sleep for the appropriate amount of time up to maxDelayMs. Ignores InterruptedException from Thread.sleep.
     */
    void sleep(int failureCount, int maxDelayMs);

    /**
     * Simple implementation of several policies parsed from a string.
     */
    public static class Standard implements BackoffPolicy {

        /**
         * Parse 'policy' as a comma separated list of between 1 and 3 items. The first argument is the name which
         * must be one of {@link Type}, the second is the max delay in ms (default 60000) and the third the base
         * delay in ms (default 1000).
         *
         * @throws IllegalArgumentException
         * @throws NumberFormatException
         */
        public static BackoffPolicy parse(String policy) {
            String[] a = policy.split("[\\s]*,[\\s]*");
            Type type = Type.valueOf(Type.class, a[0]);
            int max = a.length >= 2 ? Integer.parseInt(a[1]) : 60000;
            int base = a.length >= 3 ? Integer.parseInt(a[2]) : 1000;
            return new Standard(type, base, max);
        }

        enum Type { FIXED, LINEAR, EXPONENTIAL }

        private final Type type;
        private final int baseMs;
        private final int maxMs;

        public Standard(Type type, int baseMs, int maxMs) {
            this.type = type;
            this.baseMs = baseMs;
            this.maxMs = maxMs;
        }

        @Override
        public int getDelayMs(int failureCount) {
            if (failureCount < 1) failureCount = 1;
            int m;
            switch (type) {
                case FIXED:         m = 1;                                                  break;
                case LINEAR:        m = failureCount - 1;                                   break;
                case EXPONENTIAL:   m = 1 << (failureCount < 16 ? (failureCount - 1) : 15); break;
                default:
                    throw new IllegalStateException("Unhandled backoff type " + type);
            }
            return Math.min(maxMs, baseMs * m);
        }

        @Override
        public int getMaxDelayMs() {
            return maxMs;
        }

        @Override
        public void sleep(int failureCount) {
            sleep(failureCount, Integer.MAX_VALUE);
        }

        @Override
        public void sleep(int failureCount, int maxDelayMs) {
            try {
                Thread.sleep(Math.min(getDelayMs(failureCount), maxDelayMs));
            } catch (InterruptedException ignore) {
            }
        }
    }

}

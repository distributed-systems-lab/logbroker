package vn.huyqt.logbroker.controller;

import java.util.*;

/** Strict, shared option and fixed-voter parsing without a CLI framework. */
public final class ControllerOptions {
    private ControllerOptions() {}

    /**
     * Parses {@code --key value} pairs from {@code args[start..]}.
     *
     * @param allowed option names without the {@code --} prefix
     * @return options keyed without the prefix, in argument order
     * @throws IllegalArgumentException for an unknown, duplicated or valueless option, including a
     *     value that itself starts with {@code --}
     */
    public static Map<String, String> parse(String[] args, int start, Set<String> allowed) {
        var values = new LinkedHashMap<String, String>();
        for (int i = start; i < args.length; i += 2) {
            String key = args[i];
            if (!key.startsWith("--")
                    || !allowed.contains(key.substring(2))
                    || i + 1 >= args.length
                    || args[i + 1].startsWith("--"))
                throw new IllegalArgumentException("Unknown or missing option: " + key);
            if (values.putIfAbsent(key.substring(2), args[i + 1]) != null)
                throw new IllegalArgumentException("Duplicate option: " + key);
        }
        return values;
    }

    /**
     * Returns the value for {@code key}.
     *
     * @throws IllegalArgumentException if the option is absent or blank
     */
    public static String required(Map<String, String> options, String key) {
        var value = options.get(key);
        if (value == null || value.isBlank())
            throw new IllegalArgumentException("Missing --" + key);
        return value;
    }

    /**
     * Builds an identity from a cluster UUID and a comma-separated {@code id@host:port} voter list.
     * The port is taken after the last colon, so an IPv6 host may be written in brackets, which are
     * stripped.
     *
     * @throws IllegalArgumentException if a token is malformed, a number or the UUID does not
     *     parse, or the voters do not form a valid {@link ClusterIdentity}
     */
    public static ClusterIdentity identity(String cluster, int node, String text) {
        return identity(cluster, node, text, (short) 1);
    }

    public static ClusterIdentity identity(String cluster, int node, String text, short version) {
        var voters = new ArrayList<ClusterIdentity.Voter>();
        for (String token : text.split(",", -1)) {
            int at = token.indexOf('@'), colon = token.lastIndexOf(':');
            if (at < 1 || colon <= at + 1)
                throw new IllegalArgumentException("Voters must be id@host:port, exactly three");
            String host = token.substring(at + 1, colon);
            if (host.startsWith("[") && host.endsWith("]"))
                host = host.substring(1, host.length() - 1);
            voters.add(
                    new ClusterIdentity.Voter(
                            Integer.parseInt(token.substring(0, at)),
                            host,
                            Integer.parseInt(token.substring(colon + 1))));
        }
        return new ClusterIdentity(UUID.fromString(cluster), node, voters, version);
    }
}

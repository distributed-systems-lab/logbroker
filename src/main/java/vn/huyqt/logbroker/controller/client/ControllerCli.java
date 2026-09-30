package vn.huyqt.logbroker.controller.client;

import java.io.PrintStream;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import vn.huyqt.logbroker.broker.DeadlineScheduler;
import vn.huyqt.logbroker.controller.*;
import vn.huyqt.logbroker.controller.persistence.*;

public final class ControllerCli {
  private ControllerCli() {}

  public static void main(String[] args) {
    System.exit(run(args, System.out, System.err));
  }

  public static int run(String[] args, PrintStream out, PrintStream err) {
    try {
      if (args.length == 0) return usage(err);
      String command = args[0];
      Set<String> keys =
          switch (command) {
            case "generate-cluster-id" -> Set.of();
            case "format" -> Set.of("data", "node", "cluster", "voters");
            case "create-topic" -> Set.of("cluster", "voters", "bootstrap", "name", "partitions");
            case "metadata" -> Set.of("cluster", "voters", "bootstrap");
            case "local-metadata", "describe-quorum" ->
                Set.of("cluster", "voters", "bootstrap", "node");
            default -> throw new IllegalArgumentException("Unknown command: " + command);
          };
      var options = ControllerOptions.parse(args, 1, keys);
      return switch (command) {
        case "generate-cluster-id" -> printUuid(out);
        case "format" -> format(options, out);
        default -> admin(command, options, out);
      };
    } catch (Exception error) {
      Throwable cause = error instanceof ExecutionException ? error.getCause() : error;
      err.println("ERROR " + cause.getMessage());
      return 1;
    }
  }

  private static int printUuid(PrintStream out) {
    out.println(UUID.randomUUID());
    return 0;
  }

  private static int format(Map<String, String> options, PrintStream out) throws Exception {
    var identity =
        ControllerOptions.identity(
            ControllerOptions.required(options, "cluster"),
            Integer.parseInt(ControllerOptions.required(options, "node")),
            ControllerOptions.required(options, "voters"));
    Path root = Path.of(ControllerOptions.required(options, "data"));
    QuorumStateStore.format(root, identity, new DurableFiles());
    out.println("FORMATTED node=" + identity.nodeId() + " data=" + root.toAbsolutePath());
    return 0;
  }

  private static int admin(String command, Map<String, String> options, PrintStream out)
      throws Exception {
    String voters = ControllerOptions.required(options, "voters"),
        cluster = ControllerOptions.required(options, "cluster");
    int first = Integer.parseInt(voters.substring(0, voters.indexOf('@')));
    var identity = ControllerOptions.identity(cluster, first, voters);
    var bootstrap = new ArrayList<InetSocketAddress>();
    if (options.containsKey("bootstrap")) {
      for (String token : options.get("bootstrap").split(",")) {
        int colon = token.lastIndexOf(':');
        if (colon < 1) throw new IllegalArgumentException("Bootstrap must be host:port");
        bootstrap.add(
            new InetSocketAddress(
                token.substring(0, colon), Integer.parseInt(token.substring(colon + 1))));
      }
    } else
      for (var voter : identity.voters())
        bootstrap.add(new InetSocketAddress(voter.host(), voter.port()));
    try (var clock = DeadlineScheduler.system();
        var client =
            new ControllerClient(
                identity,
                bootstrap,
                clock,
                new NettyControllerClientTransport.Factory(identity, clock))) {
      switch (command) {
        case "create-topic" ->
            out.println(
                "CREATED topicId="
                    + client
                        .createTopic(
                            ControllerOptions.required(options, "name"),
                            Integer.parseInt(ControllerOptions.required(options, "partitions")))
                        .get(31, TimeUnit.SECONDS));
        case "metadata", "local-metadata" -> {
          var view =
              (command.equals("metadata")
                      ? client.metadata()
                      : client.localMetadata(
                          Integer.parseInt(ControllerOptions.required(options, "node"))))
                  .get(31, TimeUnit.SECONDS);
          out.println(
              "consistency="
                  + view.consistency()
                  + " node="
                  + view.nodeId()
                  + " epoch="
                  + view.epoch()
                  + " leader="
                  + view.leaderId()
                  + " commit="
                  + view.commit()
                  + " applied="
                  + view.applied());
          for (var topic : view.topics())
            out.println(
                "topic="
                    + topic.name()
                    + " topicId="
                    + topic.id()
                    + " partitions="
                    + topic.partitions());
        }
        case "describe-quorum" -> {
          var status =
              client
                  .describe(Integer.parseInt(ControllerOptions.required(options, "node")))
                  .get(31, TimeUnit.SECONDS);
          out.println(
              "node="
                  + status.nodeId()
                  + " role="
                  + status.role()
                  + " epoch="
                  + status.epoch()
                  + " leader="
                  + status.leaderId()
                  + " logEnd="
                  + status.logEnd()
                  + " durable="
                  + status.durableEnd()
                  + " commit="
                  + status.commit()
                  + " applied="
                  + status.applied()
                  + " snapshotEnd="
                  + status.snapshotEnd()
                  + " ready="
                  + status.ready()
                  + " matches="
                  + status.durableMatches()
                  + " failure="
                  + status.failure());
        }
        default -> throw new IllegalArgumentException("Unknown admin command");
      }
      return 0;
    }
  }

  private static int usage(PrintStream err) {
    err.println(
        "Commands: generate-cluster-id, format, create-topic, metadata, local-metadata, describe-quorum. Supply --cluster UUID --voters id@host:port,...; format also requires --data DIR --node ID.");
    return 2;
  }
}

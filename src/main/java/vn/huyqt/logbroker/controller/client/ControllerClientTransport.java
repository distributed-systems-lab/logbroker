package vn.huyqt.logbroker.controller.client;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import vn.huyqt.logbroker.controller.protocol.QuorumProtocol.Frame;

public interface ControllerClientTransport extends AutoCloseable {
  CompletableFuture<Void> connect(
      InetSocketAddress address, Consumer<Frame> receive, Consumer<Throwable> failure);

  CompletableFuture<Void> send(Frame frame);

  void close();

  interface Factory extends AutoCloseable {
    ControllerClientTransport create();

    default void close() {}
  }
}

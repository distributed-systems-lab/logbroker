package vn.huyqt.logbroker.transport;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import vn.huyqt.logbroker.protocol.Protocol.RequestFrame;
import vn.huyqt.logbroker.protocol.Protocol.ResponseFrame;

/** Client I/O boundary used by the Java client and transport conformance tests. */
public interface ClientTransport extends AutoCloseable {
    CompletableFuture<Void> connect(InetSocketAddress address,
            Consumer<ResponseFrame> response, Consumer<Throwable> failure);
    CompletableFuture<Void> send(RequestFrame request);
    @Override void close();
}

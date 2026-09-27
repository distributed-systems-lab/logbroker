package vn.huyqt.logbroker.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import vn.huyqt.logbroker.broker.RequestDispatcher;

/** Server lifecycle boundary independent of a network library. */
public interface ServerTransport {
    InetSocketAddress start(InetSocketAddress bind, RequestDispatcher dispatcher) throws IOException;

    void stopAccepting();

    CompletableFuture<Void> closeAsync();
}

package vn.huyqt.logbroker.support;

import vn.huyqt.logbroker.protocol.Protocol.RequestFrame;
import vn.huyqt.logbroker.protocol.Protocol.ResponseFrame;
import vn.huyqt.logbroker.transport.ClientTransport;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class LoopbackTransport implements ClientTransport {
    private final List<RequestFrame> sent = new ArrayList<>();
    private Consumer<ResponseFrame> response;
    private Consumer<Throwable> failure;

    @Override
    public CompletableFuture<Void> connect(
            InetSocketAddress address,
            Consumer<ResponseFrame> response,
            Consumer<Throwable> failure) {
        this.response = response;
        this.failure = failure;
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> send(RequestFrame frame) {
        sent.add(frame);
        return CompletableFuture.completedFuture(null);
    }

    public synchronized List<RequestFrame> sent() {
        return List.copyOf(sent);
    }

    public void reply(ResponseFrame frame) {
        response.accept(frame);
    }

    public void failConnection(Throwable cause) {
        failure.accept(cause);
    }

    @Override
    public void close() {}
}

package vn.huyqt.logbroker.controller;

import vn.huyqt.logbroker.controller.persistence.DurableFiles;

import java.net.*;
import java.nio.file.*;
import java.util.concurrent.CountDownLatch;

/**
 * Fault-test launcher uses production recovery/runtime with a private bind behind the proxy. Kept
 * in the controller package to access the package-private test bind seam.
 */
public final class ControllerProcessMain {
    public static void main(String[] args) throws Exception {
        var settings = ControllerMain.settings(Path.of(args[0]));
        var node = ControllerNode.open(settings.data(), settings.config(), new DurableFiles());
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        node.close();
                                    } catch (Exception error) {
                                        error.printStackTrace();
                                    }
                                }));
        System.out.println(
                "STARTED "
                        + node.start(
                                new InetSocketAddress("127.0.0.1", Integer.parseInt(args[1]))));
        System.out.flush();
        new CountDownLatch(1).await();
    }
}

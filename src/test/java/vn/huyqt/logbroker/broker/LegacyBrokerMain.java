package vn.huyqt.logbroker.broker;

import java.util.concurrent.CountDownLatch;

/** Process fixture for historical v1 crash and transport regressions. */
public final class LegacyBrokerMain {
    private LegacyBrokerMain() {}

    public static void main(String[] args) throws Exception {
        var broker = LegacyBrokerFixture.start(BrokerMain.parse(args));
        Runtime.getRuntime().addShutdownHook(new Thread(broker::close, "legacy-fixture-close"));
        System.out.println("READY " + broker.address().getPort());
        System.out.flush();
        new CountDownLatch(1).await();
    }
}

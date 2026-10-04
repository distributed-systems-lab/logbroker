package vn.huyqt.logbroker.integration;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import vn.huyqt.logbroker.broker.BrokerConfig;
import vn.huyqt.logbroker.broker.LegacyBrokerFixture;
import vn.huyqt.logbroker.protocol.Protocol;
import vn.huyqt.logbroker.protocol.ProtocolCodec;

import java.io.DataInputStream;
import java.net.Socket;
import java.nio.file.Path;

class BrokerBackpressureTest {
    @TempDir Path directory;

    @Test
    void connectionCapRejectsNewSocketWithoutDisturbingExistingOne() throws Exception {
        var config = BrokerConfig.defaults(directory).withPort(0).withMaxConnections(1);
        var codec = new ProtocolCodec(config.protocolLimits());
        try (var broker = LegacyBrokerFixture.start(config);
                var first = new Socket("127.0.0.1", broker.address().getPort())) {
            first.setSoTimeout(5000);
            first.getOutputStream()
                    .write(
                            codec.encodeRequest(
                                    new Protocol.RequestFrame(
                                            (short) 1,
                                            (short) 1,
                                            1,
                                            new Protocol.CreateTopic("first", 1))));
            var input = new DataInputStream(first.getInputStream());
            int length = input.readInt();
            input.skipNBytes(length);
            try (var second = new Socket("127.0.0.1", broker.address().getPort())) {
                second.setSoTimeout(5000);
                assertEquals(-1, second.getInputStream().read());
            }
            first.getOutputStream()
                    .write(
                            codec.encodeRequest(
                                    new Protocol.RequestFrame(
                                            (short) 1,
                                            (short) 1,
                                            2,
                                            new Protocol.CreateTopic("still-alive", 1))));
            length = input.readInt();
            input.skipNBytes(length);
        }
    }
}

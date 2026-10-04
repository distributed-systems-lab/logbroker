package vn.huyqt.logbroker.controller.protocol;

import static org.junit.jupiter.api.Assertions.*;

import static vn.huyqt.logbroker.controller.protocol.QuorumProtocol.*;

import org.junit.jupiter.api.Test;

import vn.huyqt.logbroker.controller.ControllerConfig;
import vn.huyqt.logbroker.controller.persistence.StateJournal;
import vn.huyqt.logbroker.controller.support.ControllerTestSupport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

class QuorumCodecLimitsTest {
    private final ControllerConfig config =
            ControllerConfig.defaults(ControllerTestSupport.identity(0));

    private byte[] valid() {
        return QuorumCodec.encode(
                new Frame(
                        (short) 107,
                        false,
                        new UUID(0, 1),
                        -1,
                        1,
                        new byte[32],
                        new CreateTopic("x", 1, 1000)));
    }

    private void recalc(byte[] bytes) {
        ByteBuffer.wrap(bytes)
                .putInt(bytes.length - 4, StateJournal.crc(bytes, 4, bytes.length - 8));
    }

    @Test
    void checksumUnsupportedVersionAndInvalidDirectionAreRejected() {
        byte[] corrupt = valid();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IOException.class, () -> QuorumCodec.decode(corrupt, config));
        byte[] version = valid();
        ByteBuffer.wrap(version).putShort(6, (short) 2);
        recalc(version);
        assertThrows(IOException.class, () -> QuorumCodec.decode(version, config));
        byte[] direction = valid();
        direction[8] = 2;
        recalc(direction);
        assertThrows(IOException.class, () -> QuorumCodec.decode(direction, config));
    }

    @Test
    void impossibleStringLengthAndTrailingPayloadAreRejectedBeforeAllocation() {
        byte[] count = valid();
        ByteBuffer.wrap(count).putInt(69, Integer.MAX_VALUE);
        recalc(count);
        assertThrows(IOException.class, () -> QuorumCodec.preflight(count, config));
        byte[] source = valid();
        byte[] trailing = java.util.Arrays.copyOf(source, source.length + 1);
        ByteBuffer.wrap(trailing).putInt(trailing.length - 4);
        recalc(trailing);
        assertThrows(IOException.class, () -> QuorumCodec.decode(trailing, config));
    }
}

package org.blip.ms.zmq.pubsub;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.blip.ms.Bytes;
import org.blip.ms.zmq.config.ZmqSenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zmq.Ctx;
import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;

import java.io.Closeable;
import java.io.IOException;

public class ZmqPubSocket implements Closeable {

    private static Logger log = LoggerFactory.getLogger(ZmqPubSocket.class);

    private final Ctx ctx;
    private final SocketBase socket;

    public ZmqPubSocket(String addr, ZmqSenderOptions options) {
        log.trace("Creating ZMQ Context for address {}", addr);
        this.ctx = ZMQ.init(1);
        if (this.ctx == null) {
            throw new RuntimeException("Unable to create ZMQ context");
        }
        options.configure(ctx);

        log.trace("Creating ZMQ socket for address {}", addr);
        this.socket = ZMQ.socket(ctx, ZMQ.ZMQ_PUB);
        if (this.socket == null) {
            throw new RuntimeException("Unable to bind ZMQ socket on " + addr);
        }
        options.configure(this.socket);

        waitBind(addr, 0);
    }

    private void waitBind(String addr, int c) {
        log.trace("Binding ZMQ socket for address {}", addr);
        boolean suc = ZMQ.bind(this.socket, addr);
        if (!suc && c >= 5) {
            log.error("Unable to bind ZMQ publishing socket using address {}", addr);
            throw new RuntimeException("Unable to bind ZMQ publishing socket using address " + addr);
        } else if (!suc) {
            log.warn(".... Unable to bind ZMQ publishing socket using address {}, trying again aftering sleeping...", addr);
            try {
                Thread.sleep(c * 2_000L);
            } catch (Exception ex) {
                // ignore
            }
            this.waitBind(addr, c+1);
        }

        // allow zero MQ some chance to bind
        try {
            Thread.sleep(2_000L);
        } catch (Exception ex) {
            // ignore
        }
    }

    public void send(final String message) {
        this.send(null, message);
    }

    public void send(final String topic, final String message) {
        send(topic, Bytes.toBytes(message));
    }

    public void send(final byte[] bits) {
        this.send(null, bits);
    }

    public void send(final String topic, final byte[] bits) {
        Msg msg = null;
        if (StringUtils.isNotBlank(topic)) {
            String header = topic + " ";
            byte[] payload = ArrayUtils.addAll(Bytes.toBytes(header), bits);
            msg = new Msg(payload);
        } else {
            msg = new Msg(bits);
        }

        boolean aok = false;

        synchronized (socket) {
            aok = socket.send(msg, ZMQ.ZMQ_DONTWAIT);
        }

        if (!aok) {
            int c = 0;
            while (!aok && c < 5) {
                synchronized (socket) {
                    aok = socket.send(msg, ZMQ.ZMQ_DONTWAIT);
                }
                c = c + 1;
            }
        }
    }

    @Override
    public void close() throws IOException {
        waitClose(0);
        waitTerm(0);
    }

    private void waitClose(int c) {
        if (c >= 5) {
            throw new RuntimeException("Attempted to close ZMQ socket failed after " + c + " attempts");
        }

        synchronized (socket) {
            try {
                ZMQ.close(this.socket);
            } catch (Exception ex) {
                this.waitClose(c+1);
            }
        }
    }

    private void waitTerm(int c) {
        if (c >= 5) {
            throw new RuntimeException("Attempted to terminate ZMQ socket failed after " + c + " attempts");
        }

        synchronized (socket) {
            try {
                ZMQ.term(this.ctx);
            } catch (Exception ex) {
                this.waitTerm(c+1);
            }
        }
    }
}

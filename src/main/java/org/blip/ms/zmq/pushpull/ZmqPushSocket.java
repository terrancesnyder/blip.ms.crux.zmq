package org.blip.ms.zmq.pushpull;

import org.blip.ms.zmq.config.ZmqSenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zmq.Ctx;
import zmq.Msg;
import zmq.SocketBase;
import zmq.ZMQ;

import java.io.Closeable;
import java.io.IOException;
import java.util.Base64;

/**
 * <p>
 * Push and Pull sockets let you distribute messages to multiple workers, arranged in a pipeline. A
 * Push socket will distribute sent messages to its Pull clients evenly. This is equivalent to
 * producer/consumer model but the results computed by consumer are not sent upstream but downstream
 * to another pull/consumer socket.
 * </p>
 *
 * <p>
 * Data always flows down the pipeline, and each stage of the pipeline is connected to at least one
 * node. When a pipeline stage is connected to multiple nodes data is load-balanced among all
 * connected nodes.
 * </p>
 *
 * <img src="https://docs-8cd05b32.s3.amazonaws.com/pushpull.png"/>
 *
 * @author Terrance A. Snyder
 *
 */
public class ZmqPushSocket implements Closeable {
    private static Logger log = LoggerFactory.getLogger(ZmqPushSocket.class);

    private final Ctx ctx;
    private final SocketBase socket;

    public ZmqPushSocket(String addr, ZmqSenderOptions options) throws IOException {

        log.trace("Creating ZMQ_PUSH Context for address {}", addr);
        this.ctx = ZMQ.init(1);
        if (this.ctx == null) {
            throw new RuntimeException("Unable to create ZMQ context");
        }
        options.configure(ctx);

        log.trace("Creating ZMQ_PUSH socket for address {}", addr);
        this.socket = ZMQ.socket(ctx, ZMQ.ZMQ_PUSH);
        if (this.socket == null) {
            throw new RuntimeException("Unable to bind ZMQ socket on " + addr);
        }
        options.configure(socket);

        log.trace("Connecting to ZMQ_PUSH socket address {}", addr);
        boolean suc = false;
        if (addr.indexOf("*:") > -1) {
            suc = ZMQ.bind(this.socket, addr);
        } else {
            suc = ZMQ.connect(this.socket, addr);
        }
        if (!suc) {
            log.error("Unable to connect to ZMQ_PUSH socket using address {}", addr);
            throw new IOException("Unable to connect to ZMQ_PUSH socket using address " + addr);
        }
        try {
            Thread.sleep(2000);
        } catch (Exception ex) {
            // ignore
        }
    }

    public void send(final String message) {
        byte[] bits = message.getBytes();
        boolean aok = false;
        synchronized (socket) {
            aok = socket.send(new Msg(bits), ZMQ.ZMQ_DONTWAIT);
        }
        if (!aok) {
            int c = 0;
            while (!aok && c < 5) {
                synchronized (socket) {
                    aok = socket.send(new Msg(bits), ZMQ.ZMQ_DONTWAIT);
                }
                c = c + 1;
            }
        }
        if (!aok) {
            log.error("Unable to send message {}", message);
        }
    }

    public void send(final byte[] bits) {
        boolean aok = false;
        synchronized (socket) {
            aok = socket.send(new Msg(bits), ZMQ.ZMQ_DONTWAIT);
        }
        if (!aok) {
            int c = 0;
            while (!aok && c < 5) {
                synchronized (socket) {
                    aok = socket.send(new Msg(bits), ZMQ.ZMQ_DONTWAIT);
                }
                c = c + 1;
            }
        }
        if (!aok) {
            log.error("Unable to send message {}", Base64.getEncoder().encode(bits));
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (socket) {
            ZMQ.close(this.socket);
            ZMQ.term(this.ctx);
        }
    }
}

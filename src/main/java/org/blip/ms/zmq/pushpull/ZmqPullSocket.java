package org.blip.ms.zmq.pushpull;

import org.blip.ms.zmq.ZmqGenericReaderSocket;
import org.blip.ms.zmq.config.ZmqConfiguration;
import org.blip.ms.zmq.config.ZmqRecieverOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zmq.Ctx;
import zmq.SocketBase;

public class ZmqPullSocket extends ZmqGenericReaderSocket {

    private static Logger log = LoggerFactory.getLogger(ZmqPullSocket.class);

    public ZmqPullSocket(final String addr, ZmqRecieverOptions options) {
        super(build(addr, options));
    }

    private static ZmqConfiguration build(final String addr, ZmqRecieverOptions options) {
        log.trace("Creating ZMQ_PULL Context for address {}", addr);
        Ctx ctx = zmq.ZMQ.init(options.ZMQ_IO_THREADS);
        if (ctx == null) {
            throw new RuntimeException("Unable to create ZMQ context");
        }
        options.configure(ctx);

        log.trace("Creating ZMQ_PULL socket for address {}", addr);
        SocketBase socket = zmq.ZMQ.socket(ctx, zmq.ZMQ.ZMQ_PULL);
        if (socket == null) {
            throw new RuntimeException("Unable to bind ZMQ socket on " + addr);
        }
        options.configure(socket);

        log.trace("Binding ZMQ_PULL socket for address {}", addr);
        boolean suc = zmq.ZMQ.bind(socket, addr);
        if (!suc) {
            log.error("Unable to bind ZMQ_PULL socket using address {}", addr);
            throw new RuntimeException("Unable to bind ZMQ_PULL socket using address " + addr);
        }

        log.trace("ZMQ_PULL socket for address {} initialized, starting service...", addr);
        ZmqConfiguration protocol = new ZmqConfiguration();
        protocol.addr = addr;
        protocol.ctx = ctx;
        protocol.socket = socket;
        protocol.topic = "";
        return protocol;
    }
}

package org.blip.ms.zmq.pubsub;

import org.blip.ms.zmq.ZmqGenericReaderSocket;
import org.blip.ms.zmq.config.ZmqConfiguration;
import org.blip.ms.zmq.config.ZmqRecieverOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zmq.Ctx;
import zmq.SocketBase;

public class ZmqSubSocket extends ZmqGenericReaderSocket {
    private static Logger log = LoggerFactory.getLogger(ZmqSubSocket.class);

    public ZmqSubSocket(String addr, ZmqRecieverOptions options) {
        this("", addr, options);
    }

    public ZmqSubSocket(String topic, String addr, ZmqRecieverOptions options) {
        super(build(topic, addr, options));
    }

    private static ZmqConfiguration build(String topic, String addr, ZmqRecieverOptions options) {
        log.trace("Creating context for ZMQ address for {}", addr);
        Ctx ctx = zmq.ZMQ.init(1);
        if (ctx == null) {
            throw new RuntimeException("Unable to create ZMQ context");
        }
        options.configure(ctx);

        log.trace("Creating socket for ZMQ address for {}", addr);
        SocketBase socket = zmq.ZMQ.socket(ctx, zmq.ZMQ.ZMQ_SUB);
        if (socket == null) {
            throw new RuntimeException("Unable to bind ZMQ socket on " + addr);
        }
        options.configure(socket);

        if (topic == null) {
            throw new RuntimeException("Can not create a socket on NULL topic, use empty string for ALL");
        }

        socket.setSocketOpt(zmq.ZMQ.ZMQ_SUBSCRIBE, topic);

        log.trace("Connecting to ZMQ address {}", addr);
        boolean suc = zmq.ZMQ.connect(socket, addr);
        if (!suc) {
            log.error("Unable to connect ZMQ subscriber to {}", addr);
            throw new RuntimeException("Unable to connect ZMQ subscriber to " + addr);
        }
        log.trace("Started ZMQ listener service for {}", addr);

        ZmqConfiguration protocol = new ZmqConfiguration();
        protocol.addr = addr;
        protocol.topic = topic;
        protocol.ctx = ctx;
        protocol.socket = socket;
        return protocol;
    }
}

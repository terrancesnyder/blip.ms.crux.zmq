package org.blip.ms.zmq.config;

import zmq.Ctx;
import zmq.SocketBase;

/**
 * Provides the specific configuration and resonable defaults for a high performance ZMQ
 * endpoint that deals with thousands of messages per second.
 *
 * @author Terrance A. Snyder
 *
 */
public class ZmqRecieverOptions extends ZmqOptions {

    /**
     * The ZMQ_RCVHWM option shall set the high water mark for inbound messages
     * on the specified socket. The high water mark is a hard limit on the
     * maximum number of outstanding messages ØMQ shall queue in memory for any
     * single peer that the specified socket is communicating with.
     */
    public int ZMQ_RCVHWM = 0;

    public void setZMQ_RCVHWM(int v) {
        this.ZMQ_RCVHWM = v;
    }

    /**
     * Sets the timeout for receive operation on the socket. If the value is
     * 0, zmq_recv(3) will return immediately, with a EAGAIN error if there
     * is no message to receive. If the value is -1, it will block until a
     * message is available. For all other values, it will wait for a
     * message for that amount of time before returning with an EAGAIN
     * error. Default: -1
     */
    public int ZMQ_RCVTIMEO = -1;
    public void setZMQ_RCVTIMEO(int v) {
        this.ZMQ_RCVTIMEO = v;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Ctx ctx) {
        super.configure(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(SocketBase socket) {
        super.configure(socket);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_RCVHWM, this.ZMQ_RCVHWM);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_RCVTIMEO, this.ZMQ_RCVTIMEO);
    }
}


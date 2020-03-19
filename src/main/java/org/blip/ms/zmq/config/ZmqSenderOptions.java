package org.blip.ms.zmq.config;

import zmq.Ctx;
import zmq.SocketBase;

public class ZmqSenderOptions extends ZmqOptions {

    /**
     * Specifies that the operation should be performed in non-blocking mode.
     * If the message cannot be queued on the socket, the zmq_send() function
     * shall fail with errno set to EAGAIN.
     */
    public int ZMQ_DONTWAIT = 1;
    public void setZMQ_DONTWAIT(int v) {
        this.ZMQ_DONTWAIT = v;
    }

    /**
     * Sets the timeout for send operation on the socket. If the value is 0,
     * zmq_send(3) will return immediately, with a EAGAIN error if the message
     * cannot be sent. If the value is -1, it will block until the message is
     * sent. For all other values, it will try to send the message for that
     * amount of time before returning with an EAGAIN error.
     */
    public int ZMQ_SNDTIMEO = 0;
    public void setZMQ_SNDTIMEO(int v) {
        this.ZMQ_SNDTIMEO = v;
    }

    /**
     * The ZMQ_SNDHWM option shall set the high water mark for outbound messages
     * on the specified socket. The high water mark is a hard limit on the
     * maximum number of outstanding messages ØMQ shall queue in memory for any
     * single peer that the specified socket is communicating with.
     */
    public int ZMQ_SNDHWM = Runtime.getRuntime().availableProcessors() * 1024;
    public void setZMQ_SNDHWM(int v) {
        this.ZMQ_SNDHWM = v;
    }

    @Override
    public void configure(Ctx ctx) {
        super.configure(ctx);
    }

    @Override
    public void configure(SocketBase socket) {
        super.configure(socket);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_SNDTIMEO, this.ZMQ_SNDTIMEO);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_SNDHWM, this.ZMQ_SNDHWM);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_LINGER, this.ZMQ_LINGER);
    }
}
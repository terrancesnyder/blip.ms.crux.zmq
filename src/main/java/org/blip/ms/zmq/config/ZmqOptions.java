package org.blip.ms.zmq.config;

import zmq.Ctx;
import zmq.SocketBase;

/**
 * General purpose options for the use with all ZMQ sockets regardless of type. Provides
 * reasonable defaults for systems that process thousands of messages per second - see implementation children.
 *
 * @author Terrance A. Snyder
 *
 */
public abstract class ZmqOptions {

    /**
     * The ZMQ_MAX_SOCKETS argument sets the maximum number of sockets
     * allowed on the context. Default: 1024
     */
    public int ZMQ_MAX_SOCKETS = 1024;
    public void setZMQ_MAX_SOCKETS(int v) {
        this.ZMQ_MAX_SOCKETS = v;
    }

    /**
     * The ZMQ_IO_THREADS argument specifies the size of the ØMQ thread pool
     * to handle I/O operations. If your application is using only the
     * inproc transport for messaging you may set this to zero, otherwise
     * set it to at least one. This option only applies before creating any
     * sockets on the context. Default: 1
     */
    public int ZMQ_IO_THREADS = Runtime.getRuntime().availableProcessors();
    public void setZMQ_IO_THREADS(int v) {
        this.ZMQ_IO_THREADS = Runtime.getRuntime().availableProcessors();
    }

    /**
     * The ZMQ_RECONNECT_IVL option shall set the initial reconnection
     * interval for the specified socket. The reconnection interval is the
     * period ØMQ shall wait between attempts to reconnect disconnected
     * peers when using connection-oriented transports.
     *
     * Default: 100 (ms)
     */
    public int ZMQ_RECONNECT_IVL = 100;
    public void setZMQ_RECONNECT_IVL(int v) {
        this.ZMQ_RECONNECT_IVL = v;
    }

    /**
     * The ZMQ_RECONNECT_IVL_MAX option shall set the maximum reconnection interval for the specified socket.
     * This is the maximum period ØMQ shall wait between attempts to reconnect. On each reconnect attempt,
     * the previous interval shall be doubled untill ZMQ_RECONNECT_IVL_MAX is reached.
     * This allows for exponential backoff strategy. Default value means no exponential backoff
     * is performed and reconnect interval calculations are only based on ZMQ_RECONNECT_IVL.
     *
     * Default: 0
     */
    public int ZMQ_RECONNECT_IVL_MAX = 0;
    public void setZMQ_RECONNECT_IVL_MAX(int v) {
        this.ZMQ_RECONNECT_IVL_MAX = v;
    }

    /**
     * The ZMQ_LINGER option shall set the linger period for the specified socket. The linger period determines how
     * long pending messages which have yet to be sent to a peer shall linger in memory after a socket is closed
     * with zmq_close(3), and further affects the termination of the socket's context with zmq_term(3).
     * The following outlines the different behaviours:
     *
     * -1 : The default value of -1 specifies an infinite linger period. Pending messages shall not be discarded after a call to zmq_close(); attempting to terminate the socket's context with zmq_term() shall block until all pending messages have been sent to a peer.
     * 0  : The value of 0 specifies no linger period. Pending messages shall be discarded immediately when the socket is closed with zmq_close().
     * >0 : Positive values specify an upper bound for the linger period in milliseconds. Pending messages shall not be discarded after a call to zmq_close(); attempting to terminate the socket's context with zmq_term() shall block until either all pending messages have been sent to a peer, or the linger period expires, after which any pending messages shall be discarded.
     */
    public int ZMQ_LINGER = 0;
    public void setZMQ_LINGER(int v) {
        this.ZMQ_LINGER = v;
    }

    /**
     * Given the current configuration apply it to the current context.
     *
     * @param ctx The ZMQ context.
     */
    public void configure(Ctx ctx) {
        zmq.ZMQ.setContextOption(ctx, zmq.ZMQ.ZMQ_MAX_SOCKETS, 64000);
        zmq.ZMQ.setContextOption(ctx, zmq.ZMQ.ZMQ_IO_THREADS, this.ZMQ_IO_THREADS);
    }

    /**
     * Given the current configuration apply it to the current socket.
     *
     * @param socket The ZMQ socket.
     */
    public void configure(SocketBase socket) {
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_LINGER, this.ZMQ_LINGER);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_RECONNECT_IVL, this.ZMQ_RECONNECT_IVL);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_RECONNECT_IVL_MAX, this.ZMQ_RECONNECT_IVL_MAX);

        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_RCVTIMEO, -1); // Sets the timeout for receive operation on the socket. Value -1 means no limit
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_SNDTIMEO, -1); // Sets the timeout for send operation on the socket. Value -1 means no limit.

        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_TCP_KEEPALIVE, TRUE);
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_TCP_KEEPALIVE_CNT, 10);       // Count time for TCP keepalive.
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_TCP_KEEPALIVE_IDLE, 30);   // Idle time for TCP keepalive.
        zmq.ZMQ.setSocketOption(socket, zmq.ZMQ.ZMQ_TCP_KEEPALIVE_INTVL, 30);    // keep alive interval (every 30 seconds)
    }

    private static final int TRUE = 1;
}

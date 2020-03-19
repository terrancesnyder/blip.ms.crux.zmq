package org.blip.ms.zmq;

import org.blip.ms.zmq.config.ZmqConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zmq.Msg;
import zmq.ZMQ;

import java.util.concurrent.BlockingQueue;

public class ZmqReaderThread extends Thread {

    private static Logger log = LoggerFactory.getLogger(ZmqReaderThread.class);

    private final ZmqConfiguration config;
    private final BlockingQueue<byte[]> bufferQ;
    private boolean shutdown = false;

    public ZmqReaderThread(ZmqConfiguration config, BlockingQueue<byte[]> bufferQ) {
        this.config = config;
        this.bufferQ = bufferQ;
    }

    public boolean isShutdown() {
        return this.shutdown;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Msg msg = null;

                synchronized (config) {
                    msg = zmq.ZMQ.recv(config.socket, ZMQ.ZMQ_DONTWAIT);
                }

                if (msg == null) {
                    continue;
                }
                byte[] bits = msg.data();
                if (bits == null) {
                    continue;
                }

                if (!bufferQ.offer(bits)) {
                    log.error("Buffer full on {}", config.addr);
                }

            } catch (Exception ex) {
                log.error("Unable to process message from address " + config.addr, ex);
            }
        }
    }
}

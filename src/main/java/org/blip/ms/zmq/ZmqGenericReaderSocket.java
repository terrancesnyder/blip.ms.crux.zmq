package org.blip.ms.zmq;

import com.google.common.collect.Queues;
import org.blip.ms.$;
import org.blip.ms.ThreadPoolFactory;
import org.blip.ms.ThreadPoolType;
import org.blip.ms.zmq.config.ZmqConfiguration;
import org.jdeferred.Promise;
import org.jdeferred.impl.DeferredObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.*;

public abstract class ZmqGenericReaderSocket implements Closeable {

    private static Logger log = LoggerFactory.getLogger(ZmqGenericReaderSocket.class);

    private final ExecutorService exec;
    private final ExecutorService callback;

    private final ZmqReaderThread reader;

    private final BlockingQueue<byte[]> queue = Queues.newLinkedBlockingQueue();

    private final DeferredObject<String, String, byte[]> dfd = $.promise();

    private final ZmqConfiguration config;

    public ZmqGenericReaderSocket(ZmqConfiguration config) {
        if (config == null) {
            throw new RuntimeException("ZMQ Configuration was not provided.");
        }
        if (!config.isValid()) {
            throw new RuntimeException("ZMQ Configuration is not valid.");
        }

        this.config = config;

        this.exec = ThreadPoolFactory.newBuilder()
                                    .setType(ThreadPoolType.FixedThreadPool)
                                    .withCoreSize(1)
                                    .withPoolSize(1)
                                    .build().create(config.getReaderThreadId(), new RejectedExecutionHandler() {
                                        @Override
                                        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                                            log.error("Rejected ZMQ sub socket: " + config.getReaderThreadId());
                                        }
                                    });

        this.callback = ThreadPoolFactory.newBuilder()
                                        .setType(ThreadPoolType.FixedThreadPool)
                                        .withCoreSize(1)
                                        .withPoolSize(1)
                                        .build()
                                        .create(config.getCallbackThreadId(), new RejectedExecutionHandler() {
                                            @Override
                                            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                                                log.error("Rejected ZMQ sub socket message: " + config.getCallbackThreadId());
                                            }
                                        });

        this.reader = new ZmqReaderThread(this.config, this.queue);
        this.exec.execute(reader);

        this.callback.submit(this::broadcast);
    }

    public Promise<String, String, byte[]> onMessage() {
        return dfd.promise();
    }

    private void broadcast() {
        while (!this.reader.isShutdown()) {
            try {
                byte[] bits = this.queue.poll(100, TimeUnit.MILLISECONDS);
                if (bits != null) {
                    this.dfd.notify(bits);
                }
            } catch (InterruptedException ex) {
                // processing was suspended / closed, ignore
            } catch (Exception ex) {
                log.error("Failure in broadcasting queue!",ex);
            }
        }
        log.trace("Broadcaster shutdown!");
    }

    @Override
    public void close() throws IOException {

        try {
            this.reader.shutdown();

            try {
                this.callback.shutdownNow();
            } catch (Exception ex) {
                log.warn("Was not able to shutdown callback worker");
            }

            try {
                this.exec.shutdownNow();
            } catch (Exception ex ) {
                log.warn("Was not able to shutdown zmq worker");
            }

            try {
                synchronized (this) {
                    zmq.ZMQ.close(this.config.socket);
                }
            } catch (Exception ex) {
                log.error("Unable to close socket", ex);
            }

            try {
                synchronized (this) {
                    zmq.ZMQ.term(this.config.ctx);
                }
            } catch (Exception ex) {
                log.error("Unable to close socket", ex);
            }
        } catch (Exception ex) {
            // ignore
            log.error("ZMQ close failure", ex);
        }
    }
}

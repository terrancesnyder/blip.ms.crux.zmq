package org.blip.ms.zmq;

import org.apache.commons.io.IOUtils;
import org.blip.ms.LocalPort;
import org.blip.ms.zmq.config.ZmqRecieverOptions;
import org.blip.ms.zmq.config.ZmqSenderOptions;
import org.blip.ms.zmq.pubsub.ZmqPubSocket;
import org.blip.ms.zmq.pubsub.ZmqSubSocket;
import org.jdeferred.ProgressCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class ZmqSubSocketTest {

    private ZmqPubSocket pub;
    private ExecutorService svc;
    private int port;

    @Rule
    public Timeout globalTimeout = new Timeout(1000 * 20);

    private static final int MAX_MESSAGES_COUNT = 10;

    @Before
    public void create_our_fake_publisher() throws IOException {
        svc = Executors.newSingleThreadExecutor();

        port = LocalPort.random().getPort();
        pub = new ZmqPubSocket("tcp://127.0.0.1:" + port, new ZmqSenderOptions());

        svc.execute(new Runnable() {
            @Override
            public void run() {
                int sentMessagesCount = 0;
                while (svc.isShutdown() == false && svc.isTerminated() == false && sentMessagesCount < MAX_MESSAGES_COUNT) {
                    System.out.println("sending raw pub/sub message...");
                    pub.send("ping", "Hello World!");
                    sentMessagesCount++;
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        });
    }

    @After
    public void close() throws InterruptedException {
        System.out.println("Trying to shutdown executorService.");
        svc.shutdown();
        svc.awaitTermination(1, TimeUnit.SECONDS);
        IOUtils.closeQuietly(pub);
        System.out.println("Closed pub.");
    }

    @Test
    public void we_can_publish_events_to_a_zmq_topic() throws Exception {
        ZmqSubSocket sub = new ZmqSubSocket("ping", "tcp://127.0.0.1:" + port, new ZmqRecieverOptions());

        try {
            final List<byte[]> recieved_messages = new ArrayList<byte[]>();
            sub.onMessage().progress(new ProgressCallback<byte[]>() {
                @Override
                public void onProgress(byte[] arg0) {
                    System.out.println("got raw pub/sub message!");
                    recieved_messages.add(arg0);
                    if (recieved_messages.size() > MAX_MESSAGES_COUNT) {
                        throw new RuntimeException("Hey, I didn't stop!");
                    }
                }
            });

            await().atMost(10, TimeUnit.SECONDS).until(() -> {
                return recieved_messages.size() >= 1;
            });
        } finally {
            IOUtils.closeQuietly(sub);
        }
    }
}

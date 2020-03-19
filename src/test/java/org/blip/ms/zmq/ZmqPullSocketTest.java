package org.blip.ms.zmq;

import org.apache.commons.io.IOUtils;
import org.blip.ms.$;
import org.blip.ms.LocalPort;
import org.blip.ms.zmq.config.ZmqRecieverOptions;
import org.blip.ms.zmq.config.ZmqSenderOptions;
import org.blip.ms.zmq.pushpull.ZmqPullSocket;
import org.blip.ms.zmq.pushpull.ZmqPushSocket;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.fest.assertions.api.Assertions.assertThat;

public class ZmqPullSocketTest {

    @Rule
    public Timeout globalTimeout = new Timeout(1000 * 10);

    @Test
    public void we_can_pull_messages() throws Exception {
        int port = LocalPort.random().getPort();

        final ZmqPushSocket pusher = new ZmqPushSocket("tcp://127.0.0.1:" + port, new ZmqSenderOptions());
        final ZmqPullSocket puller = new ZmqPullSocket("tcp://127.0.0.1:" + port, new ZmqRecieverOptions());

        try {
            final List<byte[]> received_messages = new ArrayList<byte[]>();

            final long main_thread_id = Thread.currentThread().getId();

            puller.onMessage().progress((msg) -> {
                    System.out.println("-> " + new String(msg));
                    received_messages.add(msg);
                    assertThat(main_thread_id).isNotEqualTo(Thread.currentThread().getId());
                });

            int messages_to_send = 100;

            Runnable task = () -> {
                $.trySleep(1000);
                for (int i = 0; i < messages_to_send; i++) {
                    System.out.println("sending zmq pull socket/push socket...");
                    pusher.send("Hello World message " + (i + 1) + "/" + messages_to_send + ": " + new Date().getTime());
                }
            };

            new Thread(task).start();

            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                return !received_messages.isEmpty();
            });
        } finally {
            IOUtils.closeQuietly(puller);
            IOUtils.closeQuietly(pusher);
        }
    }
}

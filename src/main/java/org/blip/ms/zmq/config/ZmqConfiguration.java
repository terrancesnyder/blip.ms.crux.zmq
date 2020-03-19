package org.blip.ms.zmq.config;

import org.apache.commons.lang3.StringUtils;
import org.blip.ms.$;
import zmq.Ctx;
import zmq.SocketBase;

public class ZmqConfiguration {
    public String addr;
    public String topic;
    public Ctx ctx;
    public SocketBase socket;

    public boolean isValid() {
        // must have address or topic
        if (StringUtils.isEmpty(addr)) {
            if (StringUtils.isEmpty(topic)) {
                return false;
            }
        }
        if (this.ctx == null) {
            return false;
        }
        if (this.socket == null) {
            return false;
        }
        return true;
    }

    public String getReaderThreadId() {
        return "zmq-" + this.addr + " (" + $.coalesce(this.topic, "uri -> " + this.addr) + ")";
    }

    public String getCallbackThreadId() {
        return "zmq-callback (" + $.coalesce(this.topic, "uri -> " + this.addr) + ")";
    }
}

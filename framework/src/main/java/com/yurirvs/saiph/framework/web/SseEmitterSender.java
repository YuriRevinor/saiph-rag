package com.yurirvs.saiph.framework.web;

import com.esotericsoftware.minlog.Log;
import jodd.util.StringUtil;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SSE EMITTER 封装
 */
public class SseEmitterSender {

    private final SseEmitter sseEmitter;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public SseEmitterSender(SseEmitter sseEmitter) {
        this.sseEmitter = sseEmitter;
        sseEmitter.onCompletion(() -> closed.set(true));
        sseEmitter.onError(throwable -> closed.set(true));
        sseEmitter.onTimeout(() -> closed.set(true));
    }

    /**
     * 发送SSE事件
     *
     * @param eventName
     * @param data
     */
    public void send(String eventName, Object data) {
        if (closed.get()) {
            return;
        }
        try {
            if (StringUtil.isBlank(eventName)) {
                sseEmitter.send(SseEmitter.event()
                        .data(data)
                        .build());
            }
            else {
                sseEmitter.send(SseEmitter.event()
                        .name(eventName)
                        .data(data)
                        .build());
            }
        }
        catch (IOException e) {
            fail(e);
        }
    }


    /**
     * 正常结束
     */
    public void complete() {
        if (closed.compareAndSet(false, true)) {
            sseEmitter.complete();
        }
    }


    /**
     * 失败时调用
     *
     * @param e
     */
    public void fail(Throwable e) {
        if (closed.compareAndSet(false, true)) {
            sseEmitter.completeWithError(e);
            Log.warn("SSEEmitter has error", e);
        }
    }


}

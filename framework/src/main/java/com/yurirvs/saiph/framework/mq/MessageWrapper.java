package com.yurirvs.saiph.framework.mq;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageWrapper<T> implements Serializable {

    /**
     * 业务 key
     */
    private String keys;

    /**
     * 业务载荷
     */
    private T body;

    /**
     * 唯一标识，用于客户端幂等验证
     */
    @Builder.Default
    private String uuid = UUID.randomUUID().toString();

    /**
     * 消息发送时间
     */
    @Builder.Default
    private Long timestamp = System.currentTimeMillis();

}

package com.yurirvs.saiph.rag.service.pipeline;

import com.yurirvs.saiph.framework.convention.ChatMessage;
import com.yurirvs.saiph.infra.chat.StreamCallback;
import com.yurirvs.saiph.rag.core.rewrite.RewriteResult;
import com.yurirvs.saiph.rag.dto.SubQuestionIntent;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * 流式对话上下文
 */
@Getter
@Builder
public class StreamChatContext {

    // ==================== 不可变输入参数 ====================

    private final String question;
    private final String conversationId;
    private final String taskId;
    private final boolean deepThinking;
    private final String userId;
    private final StreamCallback callback;

    // ==================== 管道中填充的中间状态 ====================

    @Setter
    private List<ChatMessage> history;

    @Setter
    private RewriteResult rewriteResult;

    @Setter
    private List<SubQuestionIntent> subIntents;
}

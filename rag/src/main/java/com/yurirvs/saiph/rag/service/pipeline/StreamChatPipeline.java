package com.yurirvs.saiph.rag.service.pipeline;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.yurirvs.saiph.framework.convention.ChatMessage;
import com.yurirvs.saiph.framework.convention.ChatRequest;
import com.yurirvs.saiph.infra.chat.LLMService;
import com.yurirvs.saiph.infra.chat.StreamCallback;
import com.yurirvs.saiph.infra.chat.StreamCancellationHandle;
import com.yurirvs.saiph.rag.core.guidance.GuidanceDecision;
import com.yurirvs.saiph.rag.core.guidance.IntentGuidanceService;
import com.yurirvs.saiph.rag.core.intent.IntentResolver;
import com.yurirvs.saiph.rag.core.memory.ConversationMemoryService;
import com.yurirvs.saiph.rag.core.prompt.AgentPromptResolver;
import com.yurirvs.saiph.rag.core.prompt.AgentPromptSlot;
import com.yurirvs.saiph.rag.core.retrieval.RetrievalEngine;
import com.yurirvs.saiph.rag.core.rewrite.QueryRewriteService;
import com.yurirvs.saiph.rag.core.rewrite.RewriteResult;
import com.yurirvs.saiph.rag.dto.SubQuestionIntent;
import com.yurirvs.saiph.rag.dto.RetrievalContext;
import com.yurirvs.saiph.rag.service.handler.StreamTaskManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;


/**
 * 流式对话流水线
 * <p>
 * 承载从 RAGChatServiceImpl 提取的业务编排逻辑：
 * 记忆加载 -> 改写拆分 -> 意图解析 -> 歧义引导 -> 系统响应 / 检索 -> Prompt 组装 -> 流式输出
 * <p>
 * 流水线模式：通过私有方法 + boolean 返回值（handleXxx 返回 true 表示已处理并短路）
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class StreamChatPipeline {

    private final ConversationMemoryService memoryService;
    private final QueryRewriteService queryRewriteService;
    private final IntentResolver intentResolver;
    private final IntentGuidanceService guidanceService;
    private final RetrievalEngine retrievalEngine;
    private final LLMService llmService;
    private final AgentPromptResolver agentPromptResolver;
    private final StreamTaskManager taskManager;

    public void execute(StreamChatContext ctx) {
        loadMemory(ctx);
        rewriteQuery(ctx);
        resolveIntents(ctx);

        if (handleGuidance(ctx)) {
            return;
        }
        if (handleSystemOnly(ctx)) {
            return;
        }

        RetrievalContext retrievalCtx = retrieve(ctx);
    }

    private void loadMemory(StreamChatContext ctx) {
        List<ChatMessage> history = memoryService.load(ctx.getConversationId(), ctx.getUserId());
        String questionMessageId = memoryService.append(
                ctx.getConversationId(), ctx.getUserId(), ChatMessage.user(ctx.getQuestion()));
        ctx.getCallback().onReplyToMessageId(questionMessageId);
        ctx.setHistory(history);
    }

    private void rewriteQuery(StreamChatContext ctx) {
        RewriteResult rewriteResult = queryRewriteService.rewriteWithSplit(ctx.getQuestion(), ctx.getHistory());
        ctx.setRewriteResult(rewriteResult);
    }

    private void resolveIntents(StreamChatContext ctx) {
        List<SubQuestionIntent> subIntents = intentResolver.resolve(ctx.getRewriteResult());
        ctx.setSubIntents(subIntents);
    }

    private boolean handleGuidance(StreamChatContext ctx) {
        GuidanceDecision decision = guidanceService.detectAmbiguity(
                ctx.getRewriteResult().rewrittenQuestion(),
                ctx.getSubIntents()
        );
        if (!decision.isPrompt()) {
            return false;
        }
        StreamCallback callback = ctx.getCallback();
        callback.onContent(decision.getPrompt());
        callback.onComplete();
        return true;
    }

    private boolean handleSystemOnly(StreamChatContext ctx) {
        List<SubQuestionIntent> subIntents = ctx.getSubIntents();
        boolean allSystemOnly = subIntents.stream()
                .allMatch(si -> intentResolver.isSystemOnly(si.nodeScores()));
        if (!allSystemOnly) {
            return false;
        }
        String customPrompt = subIntents.stream()
                .flatMap(si -> si.nodeScores().stream())
                .map(ns -> ns.getNode().getPromptTemplate())
                .filter(StrUtil::isNotBlank)
                .findFirst()
                .orElse(null);
        StreamCancellationHandle handle = streamSystemResponse(
                ctx.getRewriteResult().rewrittenQuestion(),
                ctx.getHistory(),
                customPrompt,
                ctx.getCallback()
        );
        taskManager.bindHandle(ctx.getTaskId(), handle);
        return true;
    }

    private RetrievalContext retrieve(StreamChatContext ctx) {
        return retrievalEngine.retrieve(ctx.getSubIntents());
    }

    private StreamCancellationHandle streamSystemResponse(String question, List<ChatMessage> history,
                                                          String customPrompt, StreamCallback callback) {
        String systemPrompt = StrUtil.isNotBlank(customPrompt)
                ? customPrompt
                : agentPromptResolver.resolve(AgentPromptSlot.SYSTEM_CHAT);

        List<ChatMessage> messages = new ArrayList<>();
        messages.add(ChatMessage.system(systemPrompt));
        if (CollUtil.isNotEmpty(history)) {
            messages.addAll(history);
        }
        messages.add(ChatMessage.user(question));

        ChatRequest req = ChatRequest.builder()
                .messages(messages)
                .temperature(0.7D)
                .thinking(false)
                .build();
        return llmService.streamChat(req, callback);
    }
}

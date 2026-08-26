package com.yurirvs.saiph.infra.chat;

import com.yurirvs.saiph.framework.convention.ChatRequest;
import com.yurirvs.saiph.framework.trace.RagTraceNode;
import com.yurirvs.saiph.infra.enums.ModelProvider;
import com.yurirvs.saiph.infra.model.ModelTarget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AIHubMixChatClient extends AbstractOpenAIStyleChatClient {

    @Override
    public String provider() {
        return ModelProvider.AI_HUB_MIX.getId();
    }

    @Override
    @RagTraceNode(name = "aihubmix-chat", type = "LLM_PROVIDER")
    public String chat(ChatRequest request, ModelTarget target) {
        return doChat(request, target);
    }

    @Override
    public StreamCancellationHandle streamChat(ChatRequest request, StreamCallback callback, ModelTarget target) {
        return doStreamChat(request, callback, target);
    }
}

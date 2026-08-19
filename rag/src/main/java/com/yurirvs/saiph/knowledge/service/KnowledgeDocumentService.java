package com.yurirvs.saiph.knowledge.service;

import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeDocumentUploadDTO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeDocumentVO;
import org.springframework.web.multipart.MultipartFile;

public interface KnowledgeDocumentService {

    /**
     * 上传文档
     *
     * @param kbId         知识库 ID
     * @param requestParam 请求对象参数
     * @param file         待上传的文件
     * @return 知识库文档视图对象
     */
    KnowledgeDocumentVO upload(String kbId, KnowledgeDocumentUploadDTO requestParam, MultipartFile file);

    void startChunk(String docId);

    /**
     * 执行文档分块（由 MQ 消费者调用）
     * 获取分布式锁 → 清理历史分块和向量 → 执行完整分块流程
     *
     * @param docId 文档 ID
     */
    void executeChunk(String docId);
}

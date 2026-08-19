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
}

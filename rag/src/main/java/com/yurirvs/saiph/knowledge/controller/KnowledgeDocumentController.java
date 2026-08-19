package com.yurirvs.saiph.knowledge.controller;


import com.yurirvs.saiph.framework.web.Result;
import com.yurirvs.saiph.framework.web.Results;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeDocumentUploadDTO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeDocumentVO;
import com.yurirvs.saiph.knowledge.service.KnowledgeDocumentService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

/**
 * RAG知识库文档管理
 */
@RestController
@RequiredArgsConstructor
public class KnowledgeDocumentController {

    private KnowledgeDocumentService documentService;

    /**
     * 上传文档：入库记录 + 文件落盘，返回文档ID
     */
    @PostMapping(value = "/knowledge-base/{kb-id}/docs/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Result<KnowledgeDocumentVO> upload(@PathVariable("kb-id") String kbId,
                                              @RequestPart(value = "file", required = false) MultipartFile file,
                                              @ModelAttribute KnowledgeDocumentUploadDTO requestParam) {
        return Results.success(documentService.upload(kbId, requestParam, file));
    }
}

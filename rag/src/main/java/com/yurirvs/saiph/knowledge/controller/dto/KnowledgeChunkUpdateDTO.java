package com.yurirvs.saiph.knowledge.controller.dto;

import lombok.Data;

/**
 * 知识库 Chunk 更新请求
 */
@Data
public class KnowledgeChunkUpdateDTO {

    /**
     * 分块正文内容
     */
    private String content;
}

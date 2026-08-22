package com.yurirvs.saiph.knowledge.controller.dto;

import lombok.Data;

/**
 * 知识库 Chunk 创建请求
 */
@Data
public class KnowledgeChunkCreateDTO {

    /**
     * 分块正文内容
     */
    private String content;

    /**
     * 下标
     */
    private Integer index;

    /**
     * 分块 ID
     */
    private String chunkId;
}

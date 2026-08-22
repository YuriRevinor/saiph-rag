package com.yurirvs.saiph.knowledge.controller.dto;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;

@Data
public class KnowledgeChunkPageDTO extends Page {

    private Integer enabled;
}

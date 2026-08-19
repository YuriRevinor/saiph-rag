package com.yurirvs.saiph.knowledge.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.yurirvs.saiph.framework.web.Result;
import com.yurirvs.saiph.framework.web.Results;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeBaseCreateDTO;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeBasePageDTO;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeBaseUpdateDTO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeBaseVO;
import com.yurirvs.saiph.knowledge.service.KnowledgeBaseService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * 知识库控制器
 * 提供知识库的增删改查等基础操作接口
 */
@RestController
@RequiredArgsConstructor
public class KnowledgeBaseController {

    private final KnowledgeBaseService knowledgeBaseService;

    /**
     * 创建知识库
     */
    @PostMapping("/knowledge-base")
    public Result<String> createKnowledgeBase(@RequestBody KnowledgeBaseCreateDTO requestParam) {
        return Results.success(knowledgeBaseService.create(requestParam));
    }

    /**
     * 重命名知识库
     */
    @PutMapping("/knowledge-base/{kb-id}")
    public Result<Void> renameKnowledgeBase(@PathVariable("kb-id") String kbId,
                                            @RequestBody KnowledgeBaseUpdateDTO requestParam) {
        knowledgeBaseService.rename(kbId, requestParam);
        return Results.success();
    }

    /**
     * 删除知识库
     */
    @DeleteMapping("/knowledge-base/{kb-id}")
    public Result<Void> deleteKnowledgeBase(@PathVariable("kb-id") String kbId) {
        knowledgeBaseService.delete(kbId);
        return Results.success();
    }

    /**
     * 查询知识库详情
     */
    @GetMapping("/knowledge-base/{kb-id}")
    public Result<KnowledgeBaseVO> queryKnowledgeBase(@PathVariable("kb-id") String kbId) {
        return Results.success(knowledgeBaseService.queryById(kbId));
    }

    /**
     * 分页查询知识库列表
     */
    @GetMapping("/knowledge-base")
    public Result<IPage<KnowledgeBaseVO>> pageQuery(KnowledgeBasePageDTO requestParam) {
        return Results.success(knowledgeBaseService.pageQuery(requestParam));
    }
}

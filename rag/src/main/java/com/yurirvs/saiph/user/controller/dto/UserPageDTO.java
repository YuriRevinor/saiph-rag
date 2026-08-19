package com.yurirvs.saiph.user.controller.dto;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.Data;

/**
 * 用户分页查询请求
 */
@Data
public class UserPageDTO extends Page {

    /**
     * 关键词（支持匹配用户名/角色）
     */
    private String keyword;
}

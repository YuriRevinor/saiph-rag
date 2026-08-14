package com.yurirvs.saiph.rag.user.controller.dto;

import lombok.Data;

/**
 * 修改密码请求
 */
@Data
public class ChangePasswordDTO {

    /**
     * 当前密码
     */
    private String currentPassword;

    /**
     * 新密码
     */
    private String newPassword;
}

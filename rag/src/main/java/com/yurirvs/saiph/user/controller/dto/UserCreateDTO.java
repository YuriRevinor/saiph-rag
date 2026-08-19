package com.yurirvs.saiph.user.controller.dto;

import lombok.Data;

/**
 * 用户创建请求
 */
@Data
public class UserCreateDTO {

    /**
     * 用户名
     */
    private String username;

    /**
     * 登录密码
     */
    private String password;

    /**
     * 角色（admin/user）
     */
    private String role;

    /**
     * 头像地址
     */
    private String avatar;
}

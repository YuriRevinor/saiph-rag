package com.yurirvs.saiph.user.enums;

/**
 * 用户角色。
 */
public enum UserRole {

    ADMIN("admin"),
    USER("user");

    /**
     * {@code t_user.role} 中存储的角色值。
     */
    private final String value;

    UserRole(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}

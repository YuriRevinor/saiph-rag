package com.yurirvs.saiph.audit.dao.entity;

import com.baomidou.mybatisplus.annotation.*;
import com.baomidou.mybatisplus.extension.handlers.JacksonTypeHandler;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName(value = "t_biz_change_log", autoResultMap = true)
public class BizChangeLogDO {

    @TableId(type = IdType.ASSIGN_ID)
    private String id;

    private String bizType;

    private String bizId;

    private String operationType;

    private String actionDesc;

    @TableField(typeHandler = JacksonTypeHandler.class)
    private String beforeSnapshot;

    @TableField(typeHandler = JacksonTypeHandler.class)
    private String afterSnapshot;

    @TableField(typeHandler = JacksonTypeHandler.class)
    private String changeDiff;

    private String operatorId;

    private String operatorName;

    private String operatorRole;

    private Boolean success;

    private String errorMessage;

    private String className;

    private String methodName;

    private String ip;

    private String userAgent;

    @TableField(fill = FieldFill.INSERT)
    private Date createTime;
}

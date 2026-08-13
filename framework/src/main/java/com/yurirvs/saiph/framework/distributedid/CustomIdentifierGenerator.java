package com.yurirvs.saiph.framework.distributedid;


import cn.hutool.core.util.IdUtil;
import com.baomidou.mybatisplus.core.incrementer.IdentifierGenerator;

/**
 * Mybatis-Plus 分布式雪花算法生成ID
 */
public class CustomIdentifierGenerator implements IdentifierGenerator {
    @Override
    public Number nextId(Object entity) {
        return IdUtil.getSnowflakeNextId();
    }

    @Override
    public String nextUUID(Object entity) {
        return IdUtil.getSnowflakeNextIdStr();
    }
}

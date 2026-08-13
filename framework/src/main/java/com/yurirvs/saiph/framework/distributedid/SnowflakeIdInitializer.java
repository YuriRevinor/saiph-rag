package com.yurirvs.saiph.framework.distributedid;


import cn.hutool.core.lang.Singleton;
import cn.hutool.core.lang.Snowflake;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * 设置HuTool雪花算法的 DatacenterId 与 WorkerId
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class SnowflakeIdInitializer {

    private final StringRedisTemplate stringRedisTemplate;

    @PostConstruct
    public void init() {

        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptSource(new ResourceScriptSource(new ClassPathResource("lua/snowflake_init.lua")));
        script.setResultType(List.class);

        try {
            List<Long> result = stringRedisTemplate.execute(script, Collections.emptyList());

            if(result.size() != 2){
                throw new RuntimeException("从Redis获取雪花算法id失败");
            }

            Long workerId = result.get(0);
            Long datacenterId = result.get(1);

            Snowflake snowflake = new Snowflake(workerId, datacenterId);
            Singleton.put(snowflake);

            log.info("分布式Snowflake初始化完成, workerId: {}, datacenterId: {}", workerId, datacenterId);
        }
        catch (Exception e) {
            throw new RuntimeException("分布式Snowflake初始化失败", e);
        }
    }
}

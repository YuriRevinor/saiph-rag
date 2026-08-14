package com.yurirvs.saiph.framework.idempotent;

import com.yurirvs.saiph.framework.exception.ServiceException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.time.Duration;

/**
 * 防止消息队列消费者重复消费消息切面控制器
 */
@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class IdempotentConsumeAspect {

    private static final String IDEMPOTENT_KEY_PREFIX = "idempotent-consume:";
    private static final String PROCESSING = "0";
    private static final String COMPLETED = "1";

    private final StringRedisTemplate stringRedisTemplate;

    /**
     * 拦截标记了 {@link IdempotentConsume} 的消息消费方法，避免同一消息被重复处理。
     */
    @Around("@annotation(idempotentConsume)")
    public Object idempotentConsume(
            ProceedingJoinPoint joinPoint, IdempotentConsume idempotentConsume) throws Throwable {
        String idempotentKey = buildIdempotentKey(joinPoint, idempotentConsume);
        long keyTimeout = idempotentConsume.keyTimeout();
        if (keyTimeout <= 0) {
            log.error("消息幂等 Key 过期时间配置非法，幂等 Key: {}，keyTimeout: {}", idempotentKey, keyTimeout);
            throw new IllegalArgumentException("IdempotentConsume keyTimeout 必须大于 0");
        }

        Boolean firstConsume = stringRedisTemplate.opsForValue()
                .setIfAbsent(idempotentKey, PROCESSING, Duration.ofSeconds(keyTimeout));
        if (!Boolean.TRUE.equals(firstConsume)) {
            return handleDuplicateConsume(idempotentKey);
        }

        Object result;
        try {
            result = joinPoint.proceed();
        } catch (Throwable throwable) {
            stringRedisTemplate.delete(idempotentKey);
            log.error("消息消费失败，已删除幂等状态，幂等 Key: {}", idempotentKey, throwable);
            throw throwable;
        }
        stringRedisTemplate.opsForValue()
                .set(idempotentKey, COMPLETED, Duration.ofSeconds(keyTimeout));
        return result;
    }

    private String buildIdempotentKey(
            ProceedingJoinPoint joinPoint, IdempotentConsume idempotentConsume) {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        Object parsedKey = SpELUtil.parseKey(idempotentConsume.key(), method, joinPoint.getArgs());
        if (parsedKey == null || !StringUtils.hasText(parsedKey.toString())) {
            log.error("消息幂等 Key 解析结果为空，消费方法: {}#{}",
                    method.getDeclaringClass().getName(), method.getName());
            throw new IllegalArgumentException("IdempotentConsume key 解析结果不能为空");
        }
        return IDEMPOTENT_KEY_PREFIX + parsedKey;
    }

    private Object handleDuplicateConsume(String idempotentKey) {
        String consumeStatus = stringRedisTemplate.opsForValue().get(idempotentKey);
        if (COMPLETED.equals(consumeStatus)) {
            log.info("消息已完成消费，跳过重复消息，幂等 Key: {}", idempotentKey);
            return null;
        }

        if (PROCESSING.equals(consumeStatus)) {
            log.warn("消息正在消费中，本次请求稍后重试，幂等 Key: {}", idempotentKey);
            throw new ServiceException("消息正在消费中，请稍后重试");
        }

        // setIfAbsent 失败后 Key 可能恰好过期，此时交由消息队列稍后重试，避免并发执行。
        log.warn("消息幂等状态已失效或未知，本次请求稍后重试，幂等 Key: {}，状态: {}",
                idempotentKey, consumeStatus);
        throw new ServiceException("消息幂等状态已失效，请稍后重试");
    }
}

package com.yurirvs.saiph.framework.idempotent;

import cn.hutool.crypto.digest.DigestUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import com.yurirvs.saiph.framework.context.UserContext;
import com.yurirvs.saiph.framework.exception.ClientException;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.reflect.MethodSignature;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;
import org.springframework.web.multipart.MultipartFile;

import java.lang.reflect.Method;

/**
 * 防止用户重复提交表单信息切面控制器
 */
@Aspect
@Component
@RequiredArgsConstructor
public final class IdempotentSubmitAspect {

    private static final String DEFAULT_LOCK_KEY_PATTERN =
            "idempotent-submit:path:%s:currentUserId:%s:md5:%s";
    private static final String CUSTOM_LOCK_KEY_PREFIX = "idempotent-submit:key:";
    private static final String ANONYMOUS_USER = "anonymous";
    private static final Gson GSON = new GsonBuilder()
            .registerTypeHierarchyAdapter(
                    MultipartFile.class,
                    (JsonSerializer<MultipartFile>) (file, type, context) -> {
                        JsonObject json = new JsonObject();
                        json.addProperty("name", file.getName());
                        json.addProperty("originalFilename", file.getOriginalFilename());
                        json.addProperty("contentType", file.getContentType());
                        json.addProperty("size", file.getSize());
                        return json;
                    })
            .create();

    private final RedissonClient redissonClient;

    /**
     * 拦截标记了 {@link IdempotentSubmit} 的方法。同一路径、用户和请求参数只允许一个请求执行；
     * 指定 {@link IdempotentSubmit#key()} 后则优先使用 SpEL 解析结果作为防重标识。
     */
    @Around("@annotation(idempotentSubmit)")
    public Object idempotentSubmit(
            ProceedingJoinPoint joinPoint, IdempotentSubmit idempotentSubmit) throws Throwable {
        String lockKey = buildLockKey(joinPoint, idempotentSubmit);
        RLock lock = redissonClient.getLock(lockKey);
        if (!lock.tryLock()) {
            throw new ClientException(idempotentSubmit.message());
        }

        try {
            return joinPoint.proceed();
        } finally {
            lock.unlock();
        }
    }

    private String buildLockKey(
            ProceedingJoinPoint joinPoint, IdempotentSubmit idempotentSubmit) {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        if (StringUtils.hasText(idempotentSubmit.key())) {
            Object parsedKey = SpELUtil.parseKey(idempotentSubmit.key(), method, joinPoint.getArgs());
            if (parsedKey == null || !StringUtils.hasText(parsedKey.toString())) {
                throw new IllegalArgumentException("IdempotentSubmit key 解析结果不能为空");
            }
            return CUSTOM_LOCK_KEY_PREFIX + parsedKey;
        }

        String requestPath = getRequestPath(method);
        String currentUserId = UserContext.getUserId();
        String userId = StringUtils.hasText(currentUserId) ? currentUserId : ANONYMOUS_USER;
        String argsDigest = DigestUtil.md5Hex(GSON.toJson(joinPoint.getArgs()));
        return String.format(DEFAULT_LOCK_KEY_PATTERN, requestPath, userId, argsDigest);
    }

    private String getRequestPath(Method method) {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
            return attributes.getRequest().getServletPath();
        }
        return method.getDeclaringClass().getName() + "#" + method.getName();
    }

}

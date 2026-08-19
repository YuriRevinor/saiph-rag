package com.yurirvs.saiph.knowledge.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.yurirvs.saiph.framework.exception.ClientException;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentScheduleDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentScheduleExecDO;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeDocumentScheduleExecMapper;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeDocumentScheduleMapper;
import com.yurirvs.saiph.knowledge.enums.SourceType;
import com.yurirvs.saiph.knowledge.schedule.CronScheduleHelper;
import com.yurirvs.saiph.knowledge.service.KnowledgeDocumentScheduleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.Date;

@Slf4j
@RequiredArgsConstructor
@Service
public class KnowledgeDocumentScheduleServiceImpl implements KnowledgeDocumentScheduleService {

    private final KnowledgeDocumentScheduleMapper scheduleMapper;
    private final KnowledgeDocumentScheduleExecMapper scheduleExecMapper;

    @Value("${rag.knowledge.schedule.min-interval-seconds:60}")
    private long scheduleMinIntervalSeconds;

    @Override
    public void upsertSchedule(KnowledgeDocumentDO documentDO) {
        // 文档首次开始处理时允许创建调度记录，已存在时则覆盖最新配置。
        syncSchedule(documentDO, true);
    }

    @Override
    public void syncScheduleIfExists(KnowledgeDocumentDO documentDO) {
        // 文档启停等状态变化只同步已有任务，避免为未配置过调度的文档创建记录。
        syncSchedule(documentDO, false);
    }

    private void syncSchedule(KnowledgeDocumentDO documentDO, boolean allowCreate) {
        // 调度记录必须关联有效的文档和知识库。
        if (documentDO == null || documentDO.getId() == null || documentDO.getKbId() == null) {
            return;
        }
        // 文件上传来源没有可重复拉取的远端地址，无需创建定时任务。
        if (!SourceType.URL.getValue().equalsIgnoreCase(documentDO.getSourceType())) {
            return;
        }

        // 只有文档与调度配置均启用且 cron 非空时，任务才可被调度器执行。
        String cron = documentDO.getScheduleCron();
        boolean documentEnabled = documentDO.getEnabled() == null || documentDO.getEnabled() == 1;
        boolean scheduleEnabled = documentDO.getScheduleEnabled() != null
                && documentDO.getScheduleEnabled() == 1
                && StringUtils.hasText(cron)
                && documentEnabled;

        Date nextRunTime = null;
        if (scheduleEnabled) {
            // 在写入任务前再次校验 cron，防止绕过接口层校验写入过于频繁或非法的配置。
            Date now = new Date();
            try {
                if (CronScheduleHelper.isIntervalLessThan(cron, now, scheduleMinIntervalSeconds)) {
                    throw new ClientException("定时周期不能小于 " + scheduleMinIntervalSeconds + " 秒");
                }
                nextRunTime = CronScheduleHelper.nextRunTime(cron, now);
            } catch (IllegalArgumentException e) {
                throw new ClientException("定时表达式不合法");
            }
        }

        // docId 在业务上唯一关联一条调度记录，查询现有记录以实现幂等写入。
        KnowledgeDocumentScheduleDO existing = scheduleMapper.selectOne(
                new LambdaQueryWrapper<KnowledgeDocumentScheduleDO>()
                        .eq(KnowledgeDocumentScheduleDO::getDocId, documentDO.getId())
                        .last("LIMIT 1")
        );
        if (existing == null) {
            // 状态同步场景不负责补建任务。
            if (!allowCreate) {
                return;
            }
            scheduleMapper.insert(KnowledgeDocumentScheduleDO.builder()
                    .docId(documentDO.getId())
                    .kbId(documentDO.getKbId())
                    .cronExpr(cron)
                    .enabled(scheduleEnabled ? 1 : 0)
                    .nextRunTime(nextRunTime)
                    .build());
            return;
        }

        // 保留历史执行状态、内容指纹及锁信息，仅刷新来自文档配置的字段。
        scheduleMapper.update(
                new LambdaUpdateWrapper<KnowledgeDocumentScheduleDO>()
                        .eq(KnowledgeDocumentScheduleDO::getId, existing.getId())
                        .set(KnowledgeDocumentScheduleDO::getCronExpr, cron)
                        .set(KnowledgeDocumentScheduleDO::getEnabled, scheduleEnabled ? 1 : 0)
                        .set(KnowledgeDocumentScheduleDO::getNextRunTime, nextRunTime)
        );
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void deleteByDocId(String docId) {
        // 空 ID 不产生数据库操作。
        if (!StringUtils.hasText(docId)) {
            return;
        }
        // 先清理执行明细，再删除主任务；事务保证两步操作同时成功或回滚。
        scheduleExecMapper.delete(
                new LambdaQueryWrapper<KnowledgeDocumentScheduleExecDO>()
                        .eq(KnowledgeDocumentScheduleExecDO::getDocId, docId)
        );
        scheduleMapper.delete(
                new LambdaQueryWrapper<KnowledgeDocumentScheduleDO>()
                        .eq(KnowledgeDocumentScheduleDO::getDocId, docId)
        );
    }
}

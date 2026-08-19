package com.yurirvs.saiph.knowledge.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.mzt.logapi.starter.annotation.LogRecord;
import com.yurirvs.saiph.audit.constant.BizChangeBizType;
import com.yurirvs.saiph.audit.constant.BizChangeOperationType;
import com.yurirvs.saiph.audit.support.BizChangeLogContext;
import com.yurirvs.saiph.core.ingest.*;
import com.yurirvs.saiph.core.parser.registry.ParserRegistry;
import com.yurirvs.saiph.framework.context.UserContext;
import com.yurirvs.saiph.framework.exception.ClientException;
import com.yurirvs.saiph.framework.exception.ServiceException;
import com.yurirvs.saiph.framework.mq.producer.MessageQueueProducer;
import com.yurirvs.saiph.ingestion.service.IngestionPipelineService;
import com.yurirvs.saiph.knowledge.config.KnowledgeScheduleProperties;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeDocumentUploadDTO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeDocumentVO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeBaseDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentChunkLogDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentDO;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeDocumentChunkLogMapper;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.yurirvs.saiph.knowledge.enums.DocumentStatus;
import com.yurirvs.saiph.knowledge.enums.ProcessMode;
import com.yurirvs.saiph.knowledge.enums.SourceType;
import com.yurirvs.saiph.knowledge.handler.RemoteFileFetcher;
import com.yurirvs.saiph.knowledge.mq.event.KnowledgeDocumentChunkEvent;
import com.yurirvs.saiph.knowledge.schedule.CronScheduleHelper;
import com.yurirvs.saiph.knowledge.service.KnowledgeDocumentScheduleService;
import com.yurirvs.saiph.knowledge.service.KnowledgeDocumentService;
import com.yurirvs.saiph.knowledge.support.IngestionSpecCodec;
import com.yurirvs.saiph.knowledge.support.VectorTargetResolver;
import com.yurirvs.saiph.rag.dto.StoredFileDTO;
import com.yurirvs.saiph.rag.service.FileStorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.Date;

@Slf4j
@Service
@RequiredArgsConstructor
public class KnowledgeDocumentServiceImpl implements KnowledgeDocumentService {

    private final KnowledgeBaseMapper knowledgeBaseMapper;
    private final KnowledgeDocumentMapper documentMapper;
    private final KnowledgeScheduleProperties scheduleProperties;
    private final IngestionSpecCodec ingestionSpecCodec;
    private final IngestionPipelineService ingestionPipelineService;
    private final BizChangeLogContext bizChangeLogContext;
    private final ParserRegistry parserRegistry;
    private final FileStorageService fileStorageService;
    private final RemoteFileFetcher remoteFileFetcher;
    private final MessageQueueProducer messageQueueProducer;
    private final KnowledgeDocumentScheduleService scheduleService;
    private final VectorTargetResolver vectorTargetResolver;
    private final KnowledgeDocumentChunkLogMapper chunkLogMapper;
    private final IngestionKernel ingestionKernel;
    private final TransactionOperations transactionOperations;

    @Value("knowledge-document-chunk_topic")
    private String chunkTopic;


    @LogRecord(
            success = "上传文档：{{#bizChangeName}}",
            fail = "上传文档失败：{{#_errorMsg}}",
            type = BizChangeBizType.KNOWLEDGE_DOCUMENT,
            subType = BizChangeOperationType.CREATE,
            bizNo = "{{#bizChangeBizId != null ? #bizChangeBizId : #kbId}}",
            extra = BizChangeLogContext.SNAPSHOT_EXPRESSION,
            condition = BizChangeLogContext.RECORD_CONDITION
    )
    @Override
    public KnowledgeDocumentVO upload(String kbId, KnowledgeDocumentUploadDTO requestParam, MultipartFile file) {
        //检查知识库
        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(kbId);
        Assert.notNull(kbDO, () -> new ClientException("知识库不存在"));
        //检查文件类型
        SourceType sourceType = SourceType.normalize(requestParam.getSourceType());
        //校验来源参数和定时同步配置
        validateSourceAndSchedule(sourceType, requestParam);
        //解析处理模式和分块策略
        ProcessModeConfig modeConfig = resolveProcessModeConfig(requestParam);
        //上传到对象存储
        StoredFileDTO stored = resolveStoredFile(kbDO.getCollectionName(), sourceType, requestParam.getSourceLocation(), file);
        // 前置拦截：与分块阶段同一套 MIME 路由，无解析器的类型直接拒绝，不落库不发 MQ
        if (!parserRegistry.canParse(stored.getMimeType())) {
            fileStorageService.deleteByUrl(stored.getUrl());
            throw new ClientException("暂不支持的文件类型：" + stored.getDetectedType());
        }

        KnowledgeDocumentDO documentDO = KnowledgeDocumentDO.builder()
                .kbId(kbId)
                .docName(stored.getOriginalFilename())
                .enabled(1)
                .chunkCount(0)
                .fileUrl(stored.getUrl())
                .fileType(stored.getDetectedType())
                .mimeType(stored.getMimeType())
                .fileSize(stored.getSize())
                .status(DocumentStatus.PENDING.getCode())
                .sourceType(sourceType.getValue())
                .sourceLocation(SourceType.URL == sourceType ? StrUtil.trimToNull(requestParam.getSourceLocation()) : null)
                .scheduleEnabled(isScheduleEnabled(sourceType, requestParam) ? 1 : 0)
                .scheduleCron(isScheduleEnabled(sourceType, requestParam) ? StrUtil.trimToNull(requestParam.getScheduleCron()) : null)
                .processMode(modeConfig.processMode().getValue())
                .ingestionSpec(modeConfig.ingestionSpec())
                .pipelineId(modeConfig.pipelineId())
                .createdBy(UserContext.getUsername())
                .updatedBy(UserContext.getUsername())
                .build();
        documentMapper.insert(documentDO);
        bizChangeLogContext.put(String.valueOf(documentDO.getId()), null, documentDO);
        bizChangeLogContext.putName(documentDO.getDocName());

        return toVO(documentDO);
    }

    @LogRecord(
            success = "开始文档分块：{{#bizChangeName}}",
            fail = "开始文档分块失败：{{#_errorMsg}}",
            type = BizChangeBizType.KNOWLEDGE_DOCUMENT,
            subType = BizChangeOperationType.RUN,
            bizNo = "{{#docId}}",
            extra = BizChangeLogContext.SNAPSHOT_EXPRESSION,
            condition = BizChangeLogContext.RECORD_CONDITION
    )
    @Override
    public void startChunk(String docId) {
        KnowledgeDocumentDO beforeDO = documentMapper.selectById(docId);
        Assert.notNull(beforeDO, () -> new ClientException("文档不存在"));
        bizChangeLogContext.putName(beforeDO.getDocName());

        KnowledgeDocumentDO before = BeanUtil.copyProperties(beforeDO, KnowledgeDocumentDO.class);
        KnowledgeDocumentChunkEvent event = KnowledgeDocumentChunkEvent.builder()
                .docId(docId)
                .operator(UserContext.getUsername())
                .build();

        messageQueueProducer.sendInTransaction(
                chunkTopic,
                docId,
                "文档分块",
                event,
                arg -> {
                    // Wrapper 更新不触发 updateTime 自动填充, 显式刷新, 使卡死恢复以分块开始时刻为基准
                    int updated = documentMapper.update(
                            new LambdaUpdateWrapper<KnowledgeDocumentDO>()
                                    .set(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
                                    .set(KnowledgeDocumentDO::getUpdatedBy, event.getOperator())
                                    .set(KnowledgeDocumentDO::getUpdateTime, new Date())
                                    .eq(KnowledgeDocumentDO::getId, docId)
                                    .ne(KnowledgeDocumentDO::getStatus, DocumentStatus.RUNNING.getCode())
                    );
                    if (updated == 0) {
                        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
                        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
                        throw new ClientException("文档分块操作正在进行中，请稍后再试");
                    }
                    KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
                    event.setKbId(documentDO.getKbId());
                    scheduleService.upsertSchedule(documentDO);
                }
        );
        bizChangeLogContext.put(docId, before, documentMapper.selectById(docId));
    }

    @Override
    public void executeChunk(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        if (documentDO == null) {
            log.warn("文档不存在，跳过分块任务, docId={}", docId);
            return;
        }

        runChunkTask(documentDO);
    }

    private void runChunkTask(KnowledgeDocumentDO documentDO) {
        String docId = documentDO.getId();
        ProcessMode processMode = ProcessMode.normalize(documentDO.getProcessMode());
        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(documentDO.getKbId());
        VectorTarget target = vectorTargetResolver.resolve(kbDO);
        IngestionSpec spec = ingestionSpecCodec.read(documentDO.getIngestionSpec());
        DocumentRef doc = documentRef(documentDO);

        KnowledgeDocumentChunkLogDO chunkLog = KnowledgeDocumentChunkLogDO.builder()
                .docId(docId)
                .status(DocumentStatus.RUNNING.getCode())
                .processMode(processMode.getValue())
                .parseProfile(spec.parseProfile().getCode())
                .pipelineId(documentDO.getPipelineId())
                .startTime(new Date())
                .build();
        chunkLogMapper.insert(chunkLog);

        long totalStartTime = System.currentTimeMillis();
        long extractDuration = 0;
        long chunkDuration = 0;
        long embedDuration = 0;
        long persistDuration = 0;

        try {
            if (ProcessMode.PIPELINE == processMode) {
                throw new ClientException("管道模式重构中，暂不可用，请改用直接分块：docId=" + docId);
            }

            IngestionOutcome outcome = ingestionKernel.run(doc, readFileBytes(documentDO), spec, target);
            extractDuration = outcome.timings().parseMillis();
            chunkDuration = outcome.timings().chunkMillis();
            embedDuration = outcome.timings().embedMillis();
            persistDuration = outcome.timings().indexMillis();
            int savedCount = outcome.chunkCount();
            // 回填字节探测出的真实 MIME；展示用的 file_type 仍由扩展名决定，两者互不导出
            refreshMimeType(docId, outcome.mimeType());

            markChunkSucceeded(docId, savedCount);
            long totalDuration = System.currentTimeMillis() - totalStartTime;
            updateChunkLog(chunkLog.getId(), DocumentStatus.SUCCESS.getCode(), savedCount,
                    extractDuration, chunkDuration, embedDuration, persistDuration, totalDuration, null);
        } catch (Exception e) {
            log.error("文档分块任务执行失败：docId={}", docId, e);
            markChunkFailed(documentDO.getId());
            long totalDuration = System.currentTimeMillis() - totalStartTime;
            updateChunkLog(chunkLog.getId(), DocumentStatus.FAILED.getCode(), 0,
                    extractDuration, chunkDuration, embedDuration, persistDuration, totalDuration, e.getMessage());
        }
    }

    private void markChunkFailed(String docId) {
        transactionOperations.executeWithoutResult(status -> {
            KnowledgeDocumentDO update = new KnowledgeDocumentDO();
            update.setId(docId);
            update.setStatus(DocumentStatus.FAILED.getCode());
            update.setUpdatedBy(UserContext.getUsername());
            documentMapper.updateById(update);
        });
    }

    private byte[] readFileBytes(KnowledgeDocumentDO documentDO) {
        try (InputStream is = fileStorageService.openStream(documentDO.getFileUrl())) {
            return is.readAllBytes();
        } catch (Exception e) {
            throw new ServiceException("读取文件内容失败：docId=" + documentDO.getId());
        }
    }

    private void updateChunkLog(String logId, String status, int chunkCount, long extractDuration,
                                long chunkDuration, long embedDuration, long persistDuration,
                                long totalDuration, String errorMessage) {
        KnowledgeDocumentChunkLogDO update = KnowledgeDocumentChunkLogDO.builder()
                .id(logId)
                .status(status)
                .chunkCount(chunkCount)
                .extractDuration(extractDuration)
                .chunkDuration(chunkDuration)
                .embedDuration(embedDuration)
                .persistDuration(persistDuration)
                .totalDuration(totalDuration)
                .errorMessage(errorMessage)
                .endTime(new Date())
                .build();
        chunkLogMapper.updateById(update);
    }

    private void markChunkSucceeded(String docId, int chunkCount) {
        documentMapper.updateById(KnowledgeDocumentDO.builder()
                .id(docId)
                .chunkCount(chunkCount)
                .status(DocumentStatus.SUCCESS.getCode())
                .updatedBy(UserContext.getUsername())
                .build());
    }

    private void refreshMimeType(String docId, String mimeType) {
        if (!StringUtils.hasText(mimeType)) {
            return;
        }
        documentMapper.updateById(KnowledgeDocumentDO.builder().id(docId).mimeType(mimeType).build());
    }

    private DocumentRef documentRef(KnowledgeDocumentDO documentDO) {
        return new DocumentRef(documentDO.getId(), documentDO.getKbId(), documentDO.getDocName());
    }

    private void validateSourceAndSchedule(SourceType sourceType, KnowledgeDocumentUploadDTO request) {
        String sourceLocation = StrUtil.trimToNull(request.getSourceLocation());
        if (SourceType.URL == sourceType && !StringUtils.hasText(sourceLocation)) {
            throw new ClientException("来源地址不能为空");
        }
        if (!isScheduleEnabled(sourceType, request)) {
            return;
        }
        String scheduleCron = StrUtil.trimToNull(request.getScheduleCron());
        if (!StringUtils.hasText(scheduleCron)) {
            throw new ClientException("定时表达式不能为空");
        }
        try {
            if (CronScheduleHelper.isIntervalLessThan(scheduleCron, new java.util.Date(), scheduleProperties.getMinIntervalSeconds())) {
                throw new ClientException("定时周期不能小于 " + scheduleProperties.getMinIntervalSeconds() + " 秒");
            }
        } catch (IllegalArgumentException e) {
            throw new ClientException("定时表达式不合法");
        }
    }

    private boolean isScheduleEnabled(SourceType sourceType, KnowledgeDocumentUploadDTO request) {
        return SourceType.URL == sourceType && Boolean.TRUE.equals(request.getScheduleEnabled());
    }

    private record ProcessModeConfig(ProcessMode processMode, String ingestionSpec, String pipelineId) {
    }

    private ProcessModeConfig resolveProcessModeConfig(KnowledgeDocumentUploadDTO request) {
        ProcessMode processMode = ProcessMode.normalize(request.getProcessMode());
        if (ProcessMode.CHUNK == processMode) {
            return new ProcessModeConfig(processMode, ingestionSpecCodec.normalize(request.getIngestionSpec()), null);
        } else {
            if (!StringUtils.hasText(request.getPipelineId())) {
                throw new ClientException("使用Pipeline模式时，必须指定Pipeline ID");
            }
            try {
                ingestionPipelineService.get(request.getPipelineId());
            } catch (Exception e) {
                throw new ClientException("指定的Pipeline不存在: " + request.getPipelineId());
            }
            return new ProcessModeConfig(processMode, null, request.getPipelineId());
        }
    }

    private StoredFileDTO resolveStoredFile(String bucketName, SourceType sourceType, String sourceLocation, MultipartFile file) {
        if (SourceType.FILE == sourceType) {
            Assert.notNull(file, () -> new ClientException("上传文件不能为空"));
            return fileStorageService.upload(bucketName, file);
        }
        return remoteFileFetcher.fetchAndStore(bucketName, sourceLocation);
    }

    private KnowledgeDocumentVO toVO(KnowledgeDocumentDO documentDO) {
        KnowledgeDocumentVO vo = BeanUtil.toBean(documentDO, KnowledgeDocumentVO.class);
        if (StringUtils.hasText(documentDO.getIngestionSpec())) {
            vo.setIngestionSpec(ingestionSpecCodec.write(ingestionSpecCodec.read(documentDO.getIngestionSpec())));
        }
        return vo;
    }
}


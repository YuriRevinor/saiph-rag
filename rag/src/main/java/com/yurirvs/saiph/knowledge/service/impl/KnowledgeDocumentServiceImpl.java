package com.yurirvs.saiph.knowledge.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.mzt.logapi.starter.annotation.LogRecord;
import com.yurirvs.saiph.audit.constant.BizChangeBizType;
import com.yurirvs.saiph.audit.constant.BizChangeOperationType;
import com.yurirvs.saiph.audit.support.BizChangeLogContext;
import com.yurirvs.saiph.core.chunk.model.EmbeddedChunk;
import com.yurirvs.saiph.core.ingest.*;
import com.yurirvs.saiph.core.ingest.sink.ChunkIndexWriter;
import com.yurirvs.saiph.core.parser.registry.ParserRegistry;
import com.yurirvs.saiph.framework.context.UserContext;
import com.yurirvs.saiph.framework.exception.ClientException;
import com.yurirvs.saiph.framework.exception.ServiceException;
import com.yurirvs.saiph.framework.mq.producer.MessageQueueProducer;
import com.yurirvs.saiph.ingestion.dao.entity.IngestionPipelineDO;
import com.yurirvs.saiph.ingestion.dao.mapper.IngestionPipelineMapper;
import com.yurirvs.saiph.ingestion.service.IngestionPipelineService;
import com.yurirvs.saiph.knowledge.config.KnowledgeScheduleProperties;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeDocumentPageDTO;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeDocumentUpdateDTO;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeDocumentUploadDTO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeDocumentChunkLogVO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeDocumentSearchVO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeDocumentVO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeBaseDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeChunkDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentChunkLogDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentDO;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeChunkMapper;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeDocumentChunkLogMapper;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.yurirvs.saiph.knowledge.enums.DocumentStatus;
import com.yurirvs.saiph.knowledge.enums.ProcessMode;
import com.yurirvs.saiph.knowledge.enums.SourceType;
import com.yurirvs.saiph.knowledge.handler.RemoteFileFetcher;
import com.yurirvs.saiph.knowledge.mq.event.KnowledgeDocumentChunkEvent;
import com.yurirvs.saiph.knowledge.schedule.CronScheduleHelper;
import com.yurirvs.saiph.knowledge.service.KnowledgeChunkService;
import com.yurirvs.saiph.knowledge.service.KnowledgeDocumentScheduleService;
import com.yurirvs.saiph.knowledge.service.KnowledgeDocumentService;
import com.yurirvs.saiph.knowledge.support.IngestionSpecCodec;
import com.yurirvs.saiph.knowledge.support.VectorTargetResolver;
import com.yurirvs.saiph.rag.core.vector.VectorStoreService;
import com.yurirvs.saiph.rag.dto.StoredFileDTO;
import com.yurirvs.saiph.rag.service.FileStorageService;
import com.yurirvs.saiph.rag.util.DisplayType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionOperations;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

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
    private final ChunkIndexWriter chunkIndexWriter;
    private final KnowledgeChunkMapper chunkMapper;
    private final KnowledgeChunkService knowledgeChunkService;
    private final VectorStoreService vectorStoreService;
    private final IngestionPipelineMapper ingestionPipelineMapper;

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
        }
        catch (Exception e) {
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
        }
        catch (Exception e) {
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
        }
        catch (IllegalArgumentException e) {
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
        }
        else {
            if (!StringUtils.hasText(request.getPipelineId())) {
                throw new ClientException("使用Pipeline模式时，必须指定Pipeline ID");
            }
            try {
                ingestionPipelineService.get(request.getPipelineId());
            }
            catch (Exception e) {
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

    public void chunkDocument(KnowledgeDocumentDO documentDO) {
        if (documentDO == null) {
            return;
        }
        runChunkTask(documentDO);
    }


    @Override
    @Transactional(rollbackFor = Exception.class)
    @LogRecord(
            success = "删除文档：{{#bizChangeName}}",
            fail = "删除文档失败：{{#_errorMsg}}",
            type = BizChangeBizType.KNOWLEDGE_DOCUMENT,
            subType = BizChangeOperationType.DELETE,
            bizNo = "{{#docId}}",
            extra = BizChangeLogContext.SNAPSHOT_EXPRESSION,
            condition = BizChangeLogContext.RECORD_CONDITION
    )
    public void delete(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
        bizChangeLogContext.putName(documentDO.getDocName());
        KnowledgeDocumentDO before = BeanUtil.copyProperties(documentDO, KnowledgeDocumentDO.class);

        // 禁止在文档分块运行时删除
        if (DocumentStatus.RUNNING.getCode().equals(documentDO.getStatus())) {
            throw new ClientException("文档正在分块中，无法删除");
        }

        scheduleService.deleteByDocId(docId);
        chunkLogMapper.delete(Wrappers.lambdaQuery(KnowledgeDocumentChunkLogDO.class)
                .eq(KnowledgeDocumentChunkLogDO::getDocId, docId));

        documentDO.setDeleted(1);
        documentDO.setUpdatedBy(UserContext.getUsername());
        documentMapper.deleteById(documentDO);

        // 一次调用覆盖全部落点：关系库块与向量都在扇出里，未来加索引后端也自动跟随
        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(documentDO.getKbId());
        chunkIndexWriter.deleteDocument(vectorTargetResolver.resolve(kbDO), documentRef(documentDO));
        deleteStoredFileQuietly(documentDO);
        bizChangeLogContext.put(docId, before, null);
    }

    private void deleteStoredFileQuietly(KnowledgeDocumentDO documentDO) {
        if (documentDO == null || !StringUtils.hasText(documentDO.getFileUrl())) {
            return;
        }
        try {
            fileStorageService.deleteByUrl(documentDO.getFileUrl());
        }
        catch (Exception e) {
            log.warn("删除文档存储文件失败, docId={}, fileUrl={}", documentDO.getId(), documentDO.getFileUrl(), e);
        }
    }

    @Override
    public KnowledgeDocumentVO get(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
        return toVO(documentDO);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    @LogRecord(
            success = "更新文档：{{#bizChangeName}}",
            fail = "更新文档失败：{{#_errorMsg}}",
            type = BizChangeBizType.KNOWLEDGE_DOCUMENT,
            subType = BizChangeOperationType.UPDATE,
            bizNo = "{{#docId}}",
            extra = BizChangeLogContext.SNAPSHOT_EXPRESSION,
            condition = BizChangeLogContext.RECORD_CONDITION
    )
    public void update(String docId, KnowledgeDocumentUpdateDTO requestParam) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
        bizChangeLogContext.putName(documentDO.getDocName());
        KnowledgeDocumentDO before = BeanUtil.copyProperties(documentDO, KnowledgeDocumentDO.class);

        // 禁止在文档分块运行时修改
        if (DocumentStatus.RUNNING.getCode().equals(documentDO.getStatus())) {
            throw new ClientException("文档正在分块中，无法修改");
        }

        String docName = requestParam == null ? null : requestParam.getDocName();
        if (!StringUtils.hasText(docName)) {
            throw new ClientException("文档名称不能为空");
        }

        LambdaUpdateWrapper<KnowledgeDocumentDO> updateWrapper = Wrappers.lambdaUpdate(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getId, documentDO.getId())
                .set(KnowledgeDocumentDO::getDocName, docName.trim())
                .set(KnowledgeDocumentDO::getUpdatedBy, UserContext.getUsername());

        // 如果传了 processMode，校验并更新处理配置
        if (StringUtils.hasText(requestParam.getProcessMode())) {
            ProcessMode processMode = ProcessMode.normalize(requestParam.getProcessMode());
            updateWrapper.set(KnowledgeDocumentDO::getProcessMode, processMode.getValue());

            if (ProcessMode.CHUNK == processMode) {
                String spec = ingestionSpecCodec.normalize(requestParam.getIngestionSpec());
                updateWrapper.setSql("ingestion_spec = CAST({0} AS jsonb)", spec);
                updateWrapper.set(KnowledgeDocumentDO::getPipelineId, null);
            }
            else {
                if (!StringUtils.hasText(requestParam.getPipelineId())) {
                    throw new ClientException("使用Pipeline模式时，必须指定Pipeline ID");
                }
                try {
                    ingestionPipelineService.get(requestParam.getPipelineId());
                }
                catch (Exception e) {
                    throw new ClientException("指定的Pipeline不存在: " + requestParam.getPipelineId());
                }
                updateWrapper.set(KnowledgeDocumentDO::getPipelineId, requestParam.getPipelineId());
                updateWrapper.set(KnowledgeDocumentDO::getIngestionSpec, null);
            }
        }

        // 处理定时调度相关字段（仅 URL 类型文档支持）
        boolean scheduleChanged = false;
        if (SourceType.URL.getValue().equalsIgnoreCase(documentDO.getSourceType())) {
            String newSourceLocation = requestParam.getSourceLocation();
            Integer newScheduleEnabled = requestParam.getScheduleEnabled();
            String newScheduleCron = requestParam.getScheduleCron();

            if (StringUtils.hasText(newSourceLocation)) {
                updateWrapper.set(KnowledgeDocumentDO::getSourceLocation, newSourceLocation.trim());
                scheduleChanged = true;
            }
            if (newScheduleEnabled != null) {
                updateWrapper.set(KnowledgeDocumentDO::getScheduleEnabled, newScheduleEnabled);
                scheduleChanged = true;
            }
            if (StringUtils.hasText(newScheduleCron)) {
                try {
                    CronScheduleHelper.nextRunTime(newScheduleCron, new Date());
                    // 验证 cron 周期不能太短（与 upsertSchedule 保持一致）
                    if (CronScheduleHelper.isIntervalLessThan(newScheduleCron, new Date(), 60)) {
                        throw new ClientException("定时周期不能小于 60 秒");
                    }
                }
                catch (IllegalArgumentException e) {
                    throw new ClientException("定时表达式不合法: " + e.getMessage());
                }
                updateWrapper.set(KnowledgeDocumentDO::getScheduleCron, newScheduleCron.trim());
                scheduleChanged = true;
            }

            // 验证：启用定时拉取时必须有 cron 和 sourceLocation
            if (scheduleChanged) {
                KnowledgeDocumentDO willBe = documentMapper.selectById(docId);
                Integer finalEnabled = newScheduleEnabled != null ? newScheduleEnabled : willBe.getScheduleEnabled();
                String finalCron = StringUtils.hasText(newScheduleCron) ? newScheduleCron.trim() : willBe.getScheduleCron();
                String finalLocation = StringUtils.hasText(newSourceLocation) ? newSourceLocation.trim() : willBe.getSourceLocation();

                if (finalEnabled != null && finalEnabled == 1) {
                    if (!StringUtils.hasText(finalCron)) {
                        throw new ClientException("启用定时拉取时必须设置定时表达式");
                    }
                    if (!StringUtils.hasText(finalLocation)) {
                        throw new ClientException("启用定时拉取时必须设置来源地址");
                    }
                }
            }
        }

        documentMapper.update(updateWrapper);

        if (scheduleChanged) {
            KnowledgeDocumentDO updated = documentMapper.selectById(docId);
            scheduleService.upsertSchedule(updated);
        }
        bizChangeLogContext.put(docId, before, documentMapper.selectById(docId));
    }

    @Override
    public IPage<KnowledgeDocumentVO> page(String kbId, KnowledgeDocumentPageDTO requestParam) {
        Page<KnowledgeDocumentDO> pageParam = new Page<>(requestParam.getCurrent(), requestParam.getSize());
        LambdaQueryWrapper<KnowledgeDocumentDO> queryWrapper = Wrappers.lambdaQuery(KnowledgeDocumentDO.class)
                .eq(KnowledgeDocumentDO::getKbId, kbId)
                .eq(KnowledgeDocumentDO::getDeleted, 0)
                .like(requestParam.getKeyword() != null && !requestParam.getKeyword().isBlank(), KnowledgeDocumentDO::getDocName, requestParam.getKeyword())
                .eq(requestParam.getStatus() != null && !requestParam.getStatus().isBlank(), KnowledgeDocumentDO::getStatus, requestParam.getStatus())
                .orderByDesc(KnowledgeDocumentDO::getCreateTime);

        IPage<KnowledgeDocumentVO> result = documentMapper.selectPage(pageParam, queryWrapper)
                .convert(this::toVO);

        List<String> docIds = result.getRecords().stream()
                .map(KnowledgeDocumentVO::getId)
                .collect(Collectors.toList());
        Set<String> editedDocIds = findEditedDocIds(docIds);
        result.getRecords().forEach(vo -> vo.setChunksEdited(editedDocIds.contains(vo.getId())));

        return result;
    }

    private Set<String> findEditedDocIds(List<String> docIds) {
        if (docIds == null || docIds.isEmpty()) {
            return Collections.emptySet();
        }
        QueryWrapper<KnowledgeChunkDO> wrapper = new QueryWrapper<>();
        wrapper.select("DISTINCT doc_id")
                .in("doc_id", docIds)
                .apply("update_time > create_time + INTERVAL '1 second'");
        return chunkMapper.selectObjs(wrapper).stream()
                .map(String::valueOf)
                .collect(Collectors.toSet());
    }

    @Override
    @LogRecord(
            success = "{{#enabled ? '启用' : '禁用'}}文档：{{#bizChangeName}}",
            fail = "修改文档启用状态失败：{{#_errorMsg}}",
            type = BizChangeBizType.KNOWLEDGE_DOCUMENT,
            subType = "{{#enabled ? 'ENABLE' : 'DISABLE'}}",
            bizNo = "{{#docId}}",
            extra = BizChangeLogContext.SNAPSHOT_EXPRESSION,
            condition = BizChangeLogContext.RECORD_CONDITION
    )
    public void enable(String docId, boolean enabled) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
        bizChangeLogContext.putName(documentDO.getDocName());
        KnowledgeDocumentDO before = BeanUtil.copyProperties(documentDO, KnowledgeDocumentDO.class);

        // 禁止在文档分块运行时修改
        if (DocumentStatus.RUNNING.getCode().equals(documentDO.getStatus())) {
            throw new ClientException("文档正在分块中，无法修改");
        }

        // 如果已经是目标状态，直接返回
        int targetEnabled = enabled ? 1 : 0;
        if (documentDO.getEnabled() != null && documentDO.getEnabled() == targetEnabled) {
            bizChangeLogContext.skip();
            return;
        }

        // 提前查知识库，两个分支都需要，避免重复查询
        KnowledgeBaseDO kbDO = knowledgeBaseMapper.selectById(documentDO.getKbId());
        String collectionName = kbDO.getCollectionName();

        // 启用时：embed 耗时较长，在事务外提前执行，避免长事务占用连接
        List<EmbeddedChunk> vectorChunks = Collections.emptyList();
        if (enabled) {
            // 向量文本取库里那份，不用展示文本重新组装——否则章节路径与表格 KV 渲染会静默丢失
            vectorChunks = knowledgeChunkService.embedPersistedChunks(docId, vectorTargetResolver.resolve(kbDO));
            if (CollUtil.isEmpty(vectorChunks)) {
                log.warn("启用文档时未找到任何 Chunk，仅更新启用状态并跳过向量重建，docId={}", docId);
            }
        }

        final List<EmbeddedChunk> finalEmbeddedChunks = vectorChunks;
        transactionOperations.executeWithoutResult(status -> {
            documentDO.setEnabled(targetEnabled);
            documentDO.setUpdatedBy(UserContext.getUsername());
            documentMapper.updateById(documentDO);
            scheduleService.syncScheduleIfExists(documentDO);
            knowledgeChunkService.updateEnabledByDocId(docId, String.valueOf(kbDO.getId()), enabled);

            if (!enabled) {
                vectorStoreService.deleteDocumentVectors(collectionName, docId);
            }
            else if (CollUtil.isNotEmpty(finalEmbeddedChunks)) {
                vectorStoreService.indexDocumentChunks(collectionName, docId, finalEmbeddedChunks);
            }
        });
        bizChangeLogContext.put(docId, before, documentMapper.selectById(docId));
    }

    @Override
    public List<KnowledgeDocumentSearchVO> search(String keyword, int limit) {
        if (!StringUtils.hasText(keyword)) {
            return Collections.emptyList();
        }

        int size = Math.min(Math.max(limit, 1), 20);
        Page<KnowledgeDocumentDO> mpPage = new Page<>(1, size);
        LambdaQueryWrapper<KnowledgeDocumentDO> qw = new LambdaQueryWrapper<KnowledgeDocumentDO>()
                .eq(KnowledgeDocumentDO::getDeleted, 0)
                .like(KnowledgeDocumentDO::getDocName, keyword)
                .orderByDesc(KnowledgeDocumentDO::getUpdateTime);

        IPage<KnowledgeDocumentDO> result = documentMapper.selectPage(mpPage, qw);
        List<KnowledgeDocumentSearchVO> records = result.getRecords().stream()
                .map(each -> BeanUtil.toBean(each, KnowledgeDocumentSearchVO.class))
                .toList();
        if (records.isEmpty()) {
            return records;
        }

        Set<String> kbIds = new HashSet<>();
        for (KnowledgeDocumentSearchVO record : records) {
            if (record.getKbId() != null) {
                kbIds.add(record.getKbId());
            }
        }
        if (kbIds.isEmpty()) {
            return records;
        }

        List<KnowledgeBaseDO> bases = knowledgeBaseMapper.selectByIds(kbIds);
        Map<String, String> nameMap = new HashMap<>();
        if (bases != null) {
            for (KnowledgeBaseDO base : bases) {
                nameMap.put(base.getId(), base.getName());
            }
        }
        for (KnowledgeDocumentSearchVO record : records) {
            record.setKbName(nameMap.get(record.getKbId()));
        }
        return records;
    }

    public IPage<KnowledgeDocumentChunkLogVO> getChunkLogs(String docId, Page<KnowledgeDocumentChunkLogVO> page) {
        Page<KnowledgeDocumentChunkLogDO> mpPage = new Page<>(page.getCurrent(), page.getSize());
        LambdaQueryWrapper<KnowledgeDocumentChunkLogDO> qw = new LambdaQueryWrapper<KnowledgeDocumentChunkLogDO>()
                .eq(KnowledgeDocumentChunkLogDO::getDocId, docId)
                .orderByDesc(KnowledgeDocumentChunkLogDO::getCreateTime);

        IPage<KnowledgeDocumentChunkLogDO> result = chunkLogMapper.selectPage(mpPage, qw);

        List<KnowledgeDocumentChunkLogDO> records = result.getRecords();
        Map<String, String> pipelineNameMap = new HashMap<>();
        if (CollUtil.isNotEmpty(records)) {
            Set<String> pipelineIds = new HashSet<>();
            for (KnowledgeDocumentChunkLogDO record : records) {
                if (record.getPipelineId() != null) {
                    pipelineIds.add(record.getPipelineId());
                }
            }
            if (!pipelineIds.isEmpty()) {
                List<IngestionPipelineDO> pipelines = ingestionPipelineMapper.selectByIds(pipelineIds);
                if (CollUtil.isNotEmpty(pipelines)) {
                    for (IngestionPipelineDO pipeline : pipelines) {
                        pipelineNameMap.put(pipeline.getId(), pipeline.getName());
                    }
                }
            }
        }

        Page<KnowledgeDocumentChunkLogVO> voPage = new Page<>(result.getCurrent(), result.getSize(), result.getTotal());
        voPage.setRecords(records.stream().map(each -> {
            KnowledgeDocumentChunkLogVO vo = BeanUtil.toBean(each, KnowledgeDocumentChunkLogVO.class);
            if (each.getPipelineId() != null) {
                vo.setPipelineName(pipelineNameMap.get(each.getPipelineId()));
            }
            Long totalDuration = each.getTotalDuration();
            if (totalDuration != null) {
                long other = getOther(each, totalDuration);
                vo.setOtherDuration(Math.max(0, other));
            }
            return vo;
        }).toList());
        return voPage;
    }

    private static long getOther(KnowledgeDocumentChunkLogDO each, Long totalDuration) {
        String mode = each.getProcessMode();
        boolean pipelineMode = ProcessMode.PIPELINE.getValue().equalsIgnoreCase(mode);
        long extract = each.getExtractDuration() == null ? 0 : each.getExtractDuration();
        long chunk = each.getChunkDuration() == null ? 0 : each.getChunkDuration();
        long embed = each.getEmbedDuration() == null ? 0 : each.getEmbedDuration();
        long persist = each.getPersistDuration() == null ? 0 : each.getPersistDuration();
        return pipelineMode
                ? totalDuration - chunk - persist
                : totalDuration - extract - chunk - embed - persist;
    }

    @Override
    public String preview(String docId) {
        KnowledgeDocumentDO documentDO = documentMapper.selectById(docId);
        Assert.notNull(documentDO, () -> new ClientException("文档不存在"));
        if (DisplayType.from(documentDO.getFileType()) != DisplayType.MARKDOWN) {
            throw new ClientException("仅支持预览 markdown 格式文档");
        }
        try (InputStream in = fileStorageService.openStream(documentDO.getFileUrl())) {
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (ClientException e) {
            throw e;
        } catch (Exception e) {
            throw new ClientException("读取文档内容失败: " + e.getMessage());
        }
    }
}


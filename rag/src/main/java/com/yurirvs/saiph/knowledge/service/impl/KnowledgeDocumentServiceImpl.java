package com.yurirvs.saiph.knowledge.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import com.mzt.logapi.starter.annotation.LogRecord;
import com.yurirvs.saiph.audit.constant.BizChangeBizType;
import com.yurirvs.saiph.audit.constant.BizChangeOperationType;
import com.yurirvs.saiph.audit.support.BizChangeLogContext;
import com.yurirvs.saiph.core.parser.registry.ParserRegistry;
import com.yurirvs.saiph.framework.context.UserContext;
import com.yurirvs.saiph.framework.exception.ClientException;
import com.yurirvs.saiph.ingestion.service.IngestionPipelineService;
import com.yurirvs.saiph.knowledge.config.KnowledgeScheduleProperties;
import com.yurirvs.saiph.knowledge.controller.dto.KnowledgeDocumentUploadDTO;
import com.yurirvs.saiph.knowledge.controller.vo.KnowledgeDocumentVO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeBaseDO;
import com.yurirvs.saiph.knowledge.dao.entity.KnowledgeDocumentDO;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeBaseMapper;
import com.yurirvs.saiph.knowledge.dao.mapper.KnowledgeDocumentMapper;
import com.yurirvs.saiph.knowledge.enums.DocumentStatus;
import com.yurirvs.saiph.knowledge.enums.ProcessMode;
import com.yurirvs.saiph.knowledge.enums.SourceType;
import com.yurirvs.saiph.knowledge.handler.RemoteFileFetcher;
import com.yurirvs.saiph.knowledge.schedule.CronScheduleHelper;
import com.yurirvs.saiph.knowledge.service.KnowledgeDocumentService;
import com.yurirvs.saiph.knowledge.support.IngestionSpecCodec;
import com.yurirvs.saiph.rag.dto.StoredFileDTO;
import com.yurirvs.saiph.rag.service.FileStorageService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

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


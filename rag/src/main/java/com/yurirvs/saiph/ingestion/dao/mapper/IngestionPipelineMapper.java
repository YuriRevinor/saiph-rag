package com.yurirvs.saiph.ingestion.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yurirvs.saiph.ingestion.dao.entity.IngestionPipelineDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface IngestionPipelineMapper extends BaseMapper<IngestionPipelineDO> {
}

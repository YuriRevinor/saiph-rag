package com.yurirvs.saiph.ingestion.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yurirvs.saiph.ingestion.dao.entity.IngestionTaskDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface IngestionTaskMapper extends BaseMapper<IngestionTaskDO> {
}

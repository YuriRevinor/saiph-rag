package com.yurirvs.saiph.rag.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yurirvs.saiph.rag.dao.entity.ConversationMessageDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface ConversationMessageMapper extends BaseMapper<ConversationMessageDO> {
}

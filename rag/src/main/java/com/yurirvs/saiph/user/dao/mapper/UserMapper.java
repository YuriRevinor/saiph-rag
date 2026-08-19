package com.yurirvs.saiph.user.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.yurirvs.saiph.user.dao.entity.UserDO;
import org.apache.ibatis.annotations.Mapper;

@Mapper
public interface UserMapper extends BaseMapper<UserDO> {
}

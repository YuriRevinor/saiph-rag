package com.yurirvs.saiph.user.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.yurirvs.saiph.user.controller.dto.ChangePasswordDTO;
import com.yurirvs.saiph.user.controller.dto.UserCreateDTO;
import com.yurirvs.saiph.user.controller.dto.UserPageDTO;
import com.yurirvs.saiph.user.controller.dto.UserUpdateDTO;
import com.yurirvs.saiph.user.controller.vo.UserVO;

public interface UserService {

    /**
     * 分页查询用户列表
     */
    IPage<UserVO> pageQuery(UserPageDTO requestParam);

    /**
     * 创建用户
     */
    String create(UserCreateDTO requestParam);

    /**
     * 更新用户
     */
    void update(String id, UserUpdateDTO requestParam);

    /**
     * 删除用户
     */
    void delete(String id);

    /**
     * 修改当前用户密码
     */
    void changePassword(ChangePasswordDTO requestParam);
}

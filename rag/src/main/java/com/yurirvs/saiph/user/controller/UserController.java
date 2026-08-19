package com.yurirvs.saiph.user.controller;


import com.baomidou.mybatisplus.core.metadata.IPage;
import com.yurirvs.saiph.framework.context.LoginUser;
import com.yurirvs.saiph.framework.context.UserContext;
import com.yurirvs.saiph.framework.web.Result;
import com.yurirvs.saiph.framework.web.Results;
import com.yurirvs.saiph.user.controller.dto.UserPageDTO;
import com.yurirvs.saiph.user.controller.vo.CurrentUserVO;
import com.yurirvs.saiph.user.controller.vo.UserVO;
import com.yurirvs.saiph.user.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    /**
     * 获取当前登录用户信息
     */
    @GetMapping("/user/me")
    public Result<CurrentUserVO> currentUser() {
        LoginUser user = UserContext.requireUser();
        return Results.success(new CurrentUserVO(
                user.getUserId(),
                user.getUsername(),
                user.getRole(),
                user.getAvatar()
        ));
    }

    /**
     * 分页查询用户列表
     */
    @GetMapping("/users")
    public Result<IPage<UserVO>> pageQuery(UserPageDTO requestParam) {
        //StpUtil.checkRole("admin");
        return Results.success(userService.pageQuery(requestParam));
    }
}

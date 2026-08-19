package com.yurirvs.saiph.user.service;

import com.yurirvs.saiph.user.controller.dto.LoginDTO;
import com.yurirvs.saiph.user.controller.vo.LoginVO;

public interface AuthService {

    LoginVO login(LoginDTO requestParam);

    void logout();
}

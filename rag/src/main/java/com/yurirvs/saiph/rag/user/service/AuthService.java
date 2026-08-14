package com.yurirvs.saiph.rag.user.service;

import com.yurirvs.saiph.rag.user.controller.dto.LoginDTO;
import com.yurirvs.saiph.rag.user.controller.vo.LoginVO;

public interface AuthService {

    LoginVO login(LoginDTO requestParam);

    void logout();
}

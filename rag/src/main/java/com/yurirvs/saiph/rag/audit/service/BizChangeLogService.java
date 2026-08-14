package com.yurirvs.saiph.rag.audit.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.yurirvs.saiph.rag.audit.controller.request.BizChangeLogPageRequest;
import com.yurirvs.saiph.rag.audit.controller.vo.BizChangeLogVO;

public interface BizChangeLogService {

    IPage<BizChangeLogVO> page(BizChangeLogPageRequest requestParam);

    BizChangeLogVO get(String id);
}

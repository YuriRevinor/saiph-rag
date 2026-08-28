package com.yurirvs.saiph.rag.core.rewrite;

import java.util.List;

public record RewriteResult(String rewrittenQuestion, List<String> subQuestions) {

}
package com.yurirvs.saiph.rag.dto;

import com.yurirvs.saiph.rag.core.intent.NodeScore;

import java.util.List;

/**
 * 子问题与其意图候选
 *
 * @param subQuestion 子问题文本
 * @param nodeScores  子问题的意图候选
 */
public record SubQuestionIntent(String subQuestion, List<NodeScore> nodeScores) {
}

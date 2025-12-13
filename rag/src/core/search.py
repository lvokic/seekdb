import logging
import os
import re
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple, Any

import dotenv

from src.integrations.embedding import generate_response as generate_embedding
from src.integrations.llm import generate_response as generate_llm_response
from src.prompt import QUERY_SYSTEM_PROMPT, QUERY_USER_PROMPT_TEMPLATE
from src.storage.oceanbase import get_or_create_client
from src.util import Answer

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

TABLE_NAME = "rag_documents"
TOP_K_RECALL = 20
RRF_K = 60
TOP_N_PAGES = 5
DEBUG_LOG_FILE = "search_debug.jsonl"

WEIGHT_FTS = 1.0
WEIGHT_VEC = 1.0


def log_search_debug(question: str, results: List[Dict]):
    """记录搜索调试日志"""
    try:
        entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "question": question,
            "top_candidates": [],
        }
        for i, res in enumerate(results[:5]):
            entry["top_candidates"].append(
                {
                    "rank": i + 1,
                    "rrf_score": round(res.get("rrf_score", 0), 6),
                    "filename": res.get("filename"),
                    "page": res.get("page"),
                    "content_preview": (res.get("content", "") or "").replace(
                        "\n", " "
                    )[:100]
                    + "...",
                }
            )
        with open(DEBUG_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def execute_vector_search(
    client, question_embedding: List[float], top_k: int
) -> List[Dict]:
    try:
        req = {
            "knn": {
                "field": "vector",
                "query_vector": question_embedding,
                "k": top_k,
                "num_candidates": top_k * 2,
            },
            "size": top_k,
            "_source": ["source_id", "content", "filename", "page"],
        }
        return client.search(index=TABLE_NAME, body=req) or []
    except Exception as e:
        logger.error(f"Vector search failed: {e}")
        return []


def execute_fts_search(client, question: str, top_k: int) -> List[Dict]:
    try:
        req = {
            "query": {
                "query_string": {
                    "fields": ["content"],
                    "query": question,
                }
            },
            "size": top_k,
            "_source": ["source_id", "content", "filename", "page"],
        }
        return client.search(index=TABLE_NAME, body=req) or []
    except Exception as e:
        logger.error(f"FTS search failed: {e}")
        return []


def reciprocal_rank_fusion(
    vector_results: List[Dict], fts_results: List[Dict]
) -> List[Dict]:
    """
    加权 RRF 算法
    Score = Weight * (1 / (k + rank))
    """
    doc_scores = defaultdict(float)
    doc_details = {}

    for rank, res in enumerate(vector_results):
        doc_id = res.get("source_id") or str(res.get("id"))
        if not doc_id:
            continue
        score = WEIGHT_VEC * (1.0 / (RRF_K + rank + 1))
        doc_scores[doc_id] += score
        doc_details[doc_id] = res

    for rank, res in enumerate(fts_results):
        doc_id = res.get("source_id") or str(res.get("id"))
        if not doc_id:
            continue
        score = WEIGHT_FTS * (1.0 / (RRF_K + rank + 1))
        doc_scores[doc_id] += score

        if doc_id not in doc_details:
            doc_details[doc_id] = res

    # 3. 排序
    sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)

    fused_results = []
    for doc_id, score in sorted_docs:
        item = doc_details[doc_id]
        item["rrf_score"] = score
        fused_results.append(item)

    return fused_results


def aggregate_top_pages(chunks: List[Dict], top_n: int = 5) -> List[Dict[str, Any]]:
    """多页聚合"""
    page_scores = defaultdict(float)
    page_texts = defaultdict(list)

    for chunk in chunks:
        fname = chunk.get("filename")
        pg = chunk.get("page")
        score = chunk.get("rrf_score", 0)
        content = chunk.get("content", "")

        key = (fname, pg)
        page_scores[key] += score
        page_texts[key].append(content)

    if not page_scores:
        return []

    sorted_pages = sorted(page_scores.items(), key=lambda x: x[1], reverse=True)

    result_pages = []
    for (fname, pg), score in sorted_pages[:top_n]:
        texts = page_texts[(fname, pg)]
        unique_texts = []
        seen = set()
        for t in texts:
            if t not in seen:
                unique_texts.append(t)
                seen.add(t)

        combined_context = "\n......\n".join(unique_texts)

        result_pages.append(
            {"filename": fname, "page": pg, "score": score, "content": combined_context}
        )

    return result_pages


def parse_llm_source(llm_text: str, top_pages: List[Dict]) -> Tuple[str, str, int]:
    """
    解析 LLM 返回的 [[SOURCE: x]] 标记，更新文件名和页码。
    返回: (clean_answer, filename, page)
    """
    # 默认使用 Top 1 (Rank 1)
    best_filename = top_pages[0]["filename"]
    best_page = top_pages[0]["page"]

    # 正则匹配 [[SOURCE: 1]] 或 [[SOURCE:1]]
    match = re.search(r"\[\[SOURCE:\s*(\d+)\]\]", llm_text, re.IGNORECASE)

    if match:
        try:
            idx = int(match.group(1)) - 1  # 转为 0-based 索引
            if 0 <= idx < len(top_pages):
                best_filename = top_pages[idx]["filename"]
                best_page = top_pages[idx]["page"]
                logger.info(
                    f"🤖 LLM Selected Source #{idx+1}: {best_filename} (p{best_page})"
                )
            else:
                logger.warning(f"LLM selected invalid source index: {idx+1}")
        except Exception:
            pass

        # 从回答中移除标记，保持整洁
        clean_text = re.sub(r"\[\[SOURCE:\s*\d+\]\]", "", llm_text).strip()
    else:
        logger.warning("LLM did not return [SOURCE: x] tag, using default Rank 1.")
        clean_text = llm_text

    return clean_text, best_filename, best_page


def search(question: str) -> Answer:
    answer = Answer(question=question)
    client = get_or_create_client()

    # 并行双路召回
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_emb = executor.submit(generate_embedding, question)
        q_vec = future_emb.result()

        if q_vec:
            future_knn = executor.submit(
                execute_vector_search, client, q_vec, TOP_K_RECALL
            )
        else:
            future_knn = None

        future_fts = executor.submit(execute_fts_search, client, question, TOP_K_RECALL)

        vec_res = future_knn.result() if future_knn else []
        fts_res = future_fts.result()

    logger.info(f"Recall: Vector={len(vec_res)}, FTS={len(fts_res)}")

    # 加权 RRF 融合排序 (FTS 权重更高)
    fused_results = reciprocal_rank_fusion(vec_res, fts_res)

    if fused_results:
        log_search_debug(question, fused_results)
    else:
        answer.answer = "抱歉，未找到相关文档。"
        return answer

    # 聚合 Top 5 页面
    top_pages_data = aggregate_top_pages(fused_results, top_n=TOP_N_PAGES)

    if not top_pages_data:
        answer.answer = "未找到有效页面信息。"
        return answer

    # 构建带编号的上下文
    context_parts = []
    for i, page_data in enumerate(top_pages_data):
        #  i+1 就是 LLM 需要引用的 SOURCE ID
        part_header = f"--- 文档片段 {i+1} (来源: {page_data['filename']} 第 {page_data['page']} 页) ---"
        context_parts.append(f"{part_header}\n{page_data['content']}")

    final_context = "\n\n".join(context_parts)

    #  LLM 生成答案
    prompt = QUERY_USER_PROMPT_TEMPLATE.format(
        context_text=final_context,
        question=question,
    )

    messages = [
        {"role": "system", "content": QUERY_SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]

    try:
        raw_response = generate_llm_response(messages)

        # 解析引用源，修正 metadata
        final_ans, final_fname, final_page = parse_llm_source(
            raw_response, top_pages_data
        )

        answer.answer = final_ans
        answer.filename = final_fname
        answer.page = final_page

    except Exception as e:
        answer.answer = f"生成答案时出错: {e}"

    return answer

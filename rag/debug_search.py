import argparse
import logging
import os
import time
from typing import List

# [修改点 1] 只导入 search，不再导入 add
from src.core import search 
from src.util import Answer, read_questions_json, write_answers

logger = logging.getLogger(__name__)

def parse_args():
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="RAG Search-Only Debugger (Skips Ingestion)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python3 debug_search.py --questions=./data/questions.json --output=./data/output_debug.json
        """,
    )

    # 保留 dataset 参数以防万一后续逻辑需要路径检查，但主要用于 VLM 读取原文件
    parser.add_argument(
        "--dataset",
        type=str,
        default="./data/dataset/",
        help="Path to dataset directory (used for VLM image extraction)",
    )

    parser.add_argument(
        "--questions",
        type=str,
        default="./data/questions.json",
        help="Path to questions/test JSON file",
    )

    parser.add_argument(
        "--output",
        type=str,
        default="./data/output.json",
        help="Path to output JSON file",
    )

    args = parser.parse_args()

    if not os.path.isdir(args.dataset):
        raise ValueError(f"Dataset directory does not exist: {args.dataset}")

    if not os.path.isfile(args.questions):
        raise ValueError(f"Questions file does not exist: {args.questions}")

    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    return args


if __name__ == "__main__":
    # 配置日志输出
    logging.basicConfig(
        level=logging.INFO, # 调试时可改为 DEBUG
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    args = parse_args()

    logger.info("========================================")
    logger.info("🚀 Starting SEARCH-ONLY Mode")
    logger.info("⚠️  Note: Ensure database is already populated via main.py or add.py")
    logger.info("========================================")
    
    logger.info(f"Dataset Dir (for VLM): {args.dataset}")
    logger.info(f"Questions File: {args.questions}")
    logger.info(f"Output File:    {args.output}")

    # [修改点 2] 移除了 add() 调用
    # logger.info("Skipping data preparation...")

    answers: List[Answer] = []

    logger.info("Loading questions...")
    questions: List[str] = read_questions_json(args.questions)
    total_q = len(questions)
    logger.info(f"Loaded {total_q} questions.")

    start_time = time.time()

    # 执行查询循环
    for idx, question in enumerate(questions, 1):
        logger.info(f"Processing [{idx}/{total_q}]: {question}")
        try:
            # 调用核心搜索函数
            ans = search(question)
            answers.append(ans)
            logger.info(f"   -> Result: {ans.filename} (p{ans.page})")
        except Exception as e:
            logger.error(f"Error processing question '{question}': {e}", exc_info=True)
            # 发生错误时添加一个空回答或错误提示，保证输出文件格式对齐
            answers.append(Answer(question=question, answer=f"Error: {str(e)}", filename="", page=0))

    duration = time.time() - start_time
    avg_time = duration / total_q if total_q > 0 else 0

    logger.info("----------------------------------------")
    logger.info(f"✅ Search completed in {duration:.2f}s (Avg: {avg_time:.2f}s/query)")
    logger.info(f"Writing results to {args.output}...")
    
    write_answers(answers, args.output)
    logger.info("Done.")
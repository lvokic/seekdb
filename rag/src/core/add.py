import glob
import logging
import os
import uuid
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict

import dotenv

from pyobvector import VECTOR, FtsIndexParam, FtsParser
from pyobvector.client.hybrid_search import HybridSearch
from sqlalchemy import VARCHAR, Column, Integer
from sqlalchemy.dialects.mysql import LONGTEXT

from src.parser.pdf import extract_text_and_images, compress_image
from src.integrations.vlm.openai_client import (
    generate_response as generate_vlm_response,
)
from src.integrations.embedding.openai_client import (
    generate_response as generate_embedding,
)
from src.split.split import split_text
from src.storage.oceanbase import get_or_create_client

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "1024"))
TABLE_NAME = "rag_documents"



def create_heap_table(client: HybridSearch, vector_dim: int):
    """
    [内核优化] 创建堆表 (Heap Table)，此时不创建任何索引。
    """
    logger.info(f"Initializing Heap Table '{TABLE_NAME}' (No Indexes yet)...")
    try:
        client.drop_table_if_exist(table_name=TABLE_NAME)
    except Exception:
        pass

    try:
        client.create_table(
            table_name=TABLE_NAME,
            columns=[
                Column("source_id", VARCHAR(64)),
                Column("filename", VARCHAR(512)),
                Column("page", Integer),
                Column("content", LONGTEXT),
                Column("vector", VECTOR(vector_dim)),
            ],
            indexes=[],
            mysql_organization="heap",
            mysql_charset="utf8mb4",
            mysql_collate="utf8mb4_unicode_ci",
        )
        logger.info(f"Heap Table '{TABLE_NAME}' created.")
    except Exception as e:
        logger.error(f"Table creation failed: {e}")
        raise e


def build_indices_bulk(client: HybridSearch):
    """
    [内核优化] 数据全部插入完成后，统一构建索引 (Bulk Build)。
    根据提供的 SDK 源码进行调用。
    """
    start_t = time.time()

    try:
        logger.info("Building Vector Index (HNSW)...")
        client.create_index(
            table_name=TABLE_NAME,
            is_vec_index=True,  # 标记为向量索引
            index_name="vec_idx",
            column_names=["vector"],  # 索引列
            vidx_params="distance=l2, type=hnsw, lib=vsag",  # HNSW 参数
        )
        logger.info("Vector Index build triggered.")
    except Exception as e:
        logger.error(f"Vector Index build failed: {e}")

    try:
        logger.info("Building Full-Text Index (IK)...")
        client.create_fts_idx_with_fts_index_param(
            table_name=TABLE_NAME,
            fts_idx_param=FtsIndexParam(
                index_name="fts_idx_content",
                field_names=["content"],
                parser_type=FtsParser.IK,
            ),
        )
        logger.info("Full-Text Index build triggered.")
    except Exception as e:
        logger.error(f"Full-Text Index build failed: {e}")

    cost = time.time() - start_t
    logger.info(f"✅ Index construction requests sent in {cost:.1f}s")


def worker_parse_pdf(pdf_path: str, page_num: int, filename: str) -> List[Dict]:
    """
    [子进程工作函数]
    负责 CPU 密集的 PDF 解析。
    """
    local_logger = logging.getLogger(f"worker-{os.getpid()}")
    results = []
    image_path = None

    try:
        text, image_path = extract_text_and_images(pdf_path, page_num)
        full_content = text

        if image_path:
            try:
                compress_image(image_path)
                vlm_prompt = (
                    "分析这张统计图。简要提取标题、核心趋势和关键数据点。忽略表格。"
                )
                caption = generate_vlm_response(vlm_prompt, images=image_path)
                if caption:
                    full_content += f"\n\n[图表趋势分析]: {caption}"
            except Exception as e:
                local_logger.warning(f"VLM fail: {e}")
            finally:
                if os.path.exists(image_path):
                    os.unlink(image_path)

        if not full_content.strip():
            return []

        chunks = split_text(full_content, max_chunk_size=800)

        # 只返回文本，Embedding 在主进程做，方便 Batch 处理
        return [{"filename": filename, "page": page_num, "content": c} for c in chunks]

    except Exception as e:
        local_logger.error(f"Parse error {filename} p{page_num}: {e}")
        if image_path and os.path.exists(image_path):
            os.unlink(image_path)
        return []


def batch_embed_and_insert(client, buffer: List[Dict]):
    """
    [RAG优化] 批量向量化 + 批量插入
    """
    if not buffer:
        return

    texts = [item["content"] for item in buffer]

    try:
        # [Batch Embedding]
        # embeddings = generate_embedding(texts)

        embeddings = [generate_embedding(txt) for txt in texts]

        db_rows = []
        for i, item in enumerate(buffer):
            if embeddings[i]:
                db_rows.append(
                    {
                        "source_id": str(uuid.uuid4()),
                        "filename": item["filename"],
                        "page": item["page"],
                        "content": item["content"],
                        "vector": embeddings[i],
                    }
                )

        # 批量写入 DB
        if db_rows:
            client.insert(table_name=TABLE_NAME, data=db_rows)

    except Exception as e:
        logger.error(f"Batch insert error: {e}")


def add(dataset_dir: str, max_worker: int = 6):
    """
    主入口函数
    """
    client = get_or_create_client()

    create_heap_table(client, EMBEDDING_DIM)

    pdf_files = glob.glob(os.path.join(dataset_dir, "*.pdf"))
    logger.info(f"Found {len(pdf_files)} PDFs. Starting Multi-Process ingestion...")

    tasks = []
    for pdf_file in pdf_files:
        try:
            from pypdf import PdfReader

            reader = PdfReader(pdf_file)
            fname = os.path.basename(pdf_file)
            for i in range(len(reader.pages)):
                tasks.append((pdf_file, i + 1, fname))
        except:
            pass

    logger.info(f"Total pages to process: {len(tasks)}")

    total_chunks = 0
    buffer = []
    BATCH_SIZE = 20  # 批处理大小

    with ProcessPoolExecutor(max_workers=max_worker) as executor:
        futures = {
            executor.submit(worker_parse_pdf, p, n, f): (f, n) for p, n, f in tasks
        }

        for future in as_completed(futures):
            try:
                raw_chunks = future.result()
                if raw_chunks:
                    buffer.extend(raw_chunks)

                    # 缓冲区满，执行 Batch Embedding + DB Insert
                    while len(buffer) >= BATCH_SIZE:
                        batch = buffer[:BATCH_SIZE]
                        buffer = buffer[BATCH_SIZE:]
                        batch_embed_and_insert(client, batch)
                        total_chunks += len(batch)

                        if total_chunks % 100 == 0:
                            logger.info(f"Progress: {total_chunks} chunks inserted...")

            except Exception as e:
                logger.error(f"Future result failed: {e}")

    # 处理剩余数据
    if buffer:
        batch_embed_and_insert(client, buffer)
        total_chunks += len(buffer)

    logger.info(f"Ingestion Data Phase Done. Total chunks: {total_chunks}")

    # 最后构建索引
    if total_chunks > 0:
        build_indices_bulk(client)
    else:
        logger.warning("No data found.")

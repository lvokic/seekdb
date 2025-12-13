import logging
import os
import re
import tempfile
from typing import List, Tuple, Optional


# pdfplumber: 表格提取
try:
    import pdfplumber

    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False
    logging.warning("pdfplumber not found. Install: pip install pdfplumber")

# PyMuPDF (fitz): 极速截图
try:
    import fitz

    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False
    logging.warning("pymupdf not found. Install: pip install pymupdf")

# pypdf
try:
    from pypdf import PdfReader

    PYPDF_AVAILABLE = True
except ImportError:
    PYPDF_AVAILABLE = False

# Pillow: 图片处理与压缩
try:
    from PIL import Image

    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    """
    清洗文本中的页眉页脚、免责声明等噪音。
    针对金融研报进行了特定优化。
    """
    if not text:
        return ""

    # 针对财报/研报常见的噪音模式
    noise_patterns = [
        r"\|?\s*方正证券",
        r"FOUNDER SECURITIES",
        r"正在你身边",
        r"敬请关注文后特别声明",
        r"分析师声明",
        r"免责声明",
        r"第\s*\d+\s*页",
        r"Page\s*\d+",
        r"^\s*\d+\s*$",  # 纯数字行(页码)
        r"数据来源：.*",
        r"资料来源：.*",
    ]

    for pattern in noise_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.MULTILINE)

    # 合并多余空行
    text = re.sub(r"\n{3,}", "\n\n", text)
    # 去除行首行尾空白
    lines = [line.strip() for line in text.split("\n")]
    text = "\n".join(lines)

    return text.strip()


def extract_tables_to_markdown(page) -> str:
    """
    使用 pdfplumber 提取表格并转换为 Markdown 格式文本。
    LLM 对 Markdown 表格的理解能力远强于无结构的文本流。
    """
    md_tables = []
    try:
        # extract_tables 会自动识别页面表格结构
        # snap_tolerance 适当调大可以提高表格识别率
        tables = page.extract_tables(
            table_settings={
                "vertical_strategy": "lines",
                "horizontal_strategy": "lines",
            }
        )

        for table in tables:
            if not table or len(table) < 2 or len(table[0]) < 2:
                continue

            # 清洗单元格内容
            clean_table = [
                [
                    str(cell).replace("\n", " ").strip() if cell is not None else ""
                    for cell in row
                ]
                for row in table
            ]

            if not any("".join(row) for row in clean_table):
                continue

            # 构建 Markdown 表格字符串
            try:
                # 表头
                header = "| " + " | ".join(clean_table[0]) + " |"
                # 分隔符
                separator = "| " + " | ".join(["---"] * len(clean_table[0])) + " |"
                # 数据行
                body_rows = ["| " + " | ".join(row) + " |" for row in clean_table[1:]]
                body = "\n".join(body_rows)

                md_table = f"\n\n[结构化表格数据]:\n{header}\n{separator}\n{body}\n"
                md_tables.append(md_table)
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"Table extraction skipped: {e}")

    return "\n".join(md_tables)


def is_complex_chart_page(page, text_content: str) -> bool:
    """
    智能判断是否需要调用 VLM (Vision Language Model)。
    仅当页面包含'图表'关键词，且包含大量非表格的矢量绘图时才触发。
    """
    # 1. 目录页/纯文本页直接跳过 (正则匹配目录特征)
    if re.search(
        r"(图表目录|目\s*录|CONTENTS|摘要|Abstract)\s*$",
        text_content[:500],
        flags=re.MULTILINE | re.IGNORECASE,
    ):
        return False

    # 2. 关键词检测 (图/Figure/Chart/趋势)
    has_chart_keyword = re.search(
        r"(?:图|Figure|Chart|趋势|走势|变动)\s*\d*", text_content, re.IGNORECASE
    )

    if not has_chart_keyword:
        return False

    # 3. 视觉密度检测 (Visual Density Check)
    try:
        # 统计矢量对象数量
        visual_score = len(page.rects) + len(page.lines) + len(page.curves)

        # 阈值判断：如果绘图元素 > 50，且有关键词，极大概率是统计图
        # (普通表格也有线条，但通常没有统计图那么密集)
        if visual_score > 50:
            return True

        # 检查是否有大尺寸图片对象 (位图)
        for img in page.images:
            # 简单的逻辑：如果图片高度或宽度占据页面一定比例
            if img.get("height", 0) > 200 or img.get("width", 0) > 200:
                return True

    except Exception:
        return False

    return False


def pdf_page_to_image(pdf_path: str, page_num: int, dpi: int = 72) -> Optional[str]:
    """
    使用 PyMuPDF (fitz) 将 PDF 页面转为图片。
    """
    if not PYMUPDF_AVAILABLE:
        logger.error("PyMuPDF (fitz) not installed. Cannot snapshot page.")
        return None

    try:
        # fitz.open 很快，因为它只是读取文件头
        doc = fitz.open(pdf_path)
        # fitz 使用 0-indexed
        if page_num - 1 >= len(doc):
            return None

        page = doc[page_num - 1]

        # 设置缩放矩阵
        # zoom = 1.0 -> 72 DPI, zoom = 1.38 -> ~100 DPI
        zoom = dpi / 72
        mat = fitz.Matrix(zoom, zoom)

        # 渲染为 Pixmap
        pix = page.get_pixmap(matrix=mat, alpha=False)

        # 保存到临时文件
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        # 直接存为 JPG，压缩率高
        pix.save(temp_file.name, output="jpg", jpg_quality=80)
        temp_file.close()

        doc.close()
        return temp_file.name

    except Exception as e:
        logger.error(f"Snapshot failed for {pdf_path} p{page_num}: {e}")
        return None


def compress_image(image_path: str, **kwargs) -> str:
    """
    图片压缩工具。如果图片过大，进行缩放和压缩，防止 VLM 请求超时。
    """
    if not image_path or not os.path.exists(image_path):
        return image_path

    if not PIL_AVAILABLE:
        return image_path

    try:
        # 检查文件大小
        if os.path.getsize(image_path) < 300 * 1024:
            return image_path

        with Image.open(image_path) as img:
            # 转换为 RGB (防止 RGBA 存 JPG 报错)
            if img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            # 限制最大边长为 1024
            max_dimension = 1024
            if max(img.size) > max_dimension:
                img.thumbnail((max_dimension, max_dimension))

            # 覆盖保存，降低质量以换取速度
            img.save(image_path, "JPEG", quality=75, optimize=True)

    except Exception as e:
        logger.warning(f"Image compression warning: {e}")

    return image_path


def extract_text_and_images(pdf_path: str, page_num: int) -> Tuple[str, Optional[str]]:
    """
    智能解析
    Returns:
        (text_content, image_path_or_None)
        - text_content: 包含纯文本 + Markdown 表格
        - image_path: 仅当检测到复杂趋势图时返回路径，否则为 None
    """
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF missing: {pdf_path}")

    text_content = ""
    image_path = None

    # 优先使用 pdfplumber
    if PDFPLUMBER_AVAILABLE:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # 检查页码范围
                if page_num - 1 >= len(pdf.pages):
                    return "", None

                page = pdf.pages[page_num - 1]

                # 1. 提取纯文本
                raw_text = page.extract_text() or ""
                cleaned_text = clean_text(raw_text)

                # 2. 提取表格并转 Markdown
                table_markdown = extract_tables_to_markdown(page)

                # 合并：文本在前，表格在后
                text_content = cleaned_text + "\n" + table_markdown

                # 3. 判断是否需要截图给 VLM (趋势图/统计图)
                if is_complex_chart_page(page, cleaned_text):
                    # 只有真的需要看图时，才调用 PyMuPDF 截图
                    # 默认 DPI=72，足够看清趋势，且速度极快
                    image_path = pdf_page_to_image(pdf_path, page_num, dpi=72)

        except Exception as e:
            logger.error(f"pdfplumber parse error {pdf_path} p{page_num}: {e}")

    # 策略 B: pypdf 作为备选方案
    if not text_content and PYPDF_AVAILABLE:
        try:
            reader = PdfReader(pdf_path)
            if page_num - 1 < len(reader.pages):
                raw_text = reader.pages[page_num - 1].extract_text() or ""
                text_content = clean_text(raw_text)
                # 简单兜底：如果包含“图”字且文本极少，尝试截图
                if len(text_content) < 200 and re.search(
                    r"(?:图|Figure)\s*\d+", text_content
                ):
                    image_path = pdf_page_to_image(pdf_path, page_num)
        except Exception as e:
            logger.error(f"pypdf parse error: {e}")

    return text_content, image_path


def extract_images_from_page(pdf_path: str, page_num: int) -> List[str]:
    """ """
    return []

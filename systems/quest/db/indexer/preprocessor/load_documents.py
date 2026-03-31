import sys
import os
import glob
from typing import Dict, List
from pathlib import Path
import pdfplumber

from quest.core.datapack.doc import TextDoc, ZenDBDoc

# 模块1: 文档处理与SHT构建
def util_load_zendb_docs(paths, debug_flag=False, topK = 1, start_doc_id = 1) -> List[ZenDBDoc]:
    """解析PDF或TXT文本和视觉特征"""
    print("\n📖 解析文档...")
    docs = []
    for i, path in enumerate(paths):
        if debug_flag and i >= topK:  # 限制调试时只处理前topK个文档
            print(f"    🔍 调试模式：只处理前{topK}个文档")
            break
        ext = Path(path).suffix.lower()
        # print(f"  处理文档 {i+1}/{len(paths)}: {Path(path).name}")
        print(f" filename: {Path(path).name}, doc_id: {i+start_doc_id}: ") # (文档文件名, doc_id)

        try:
            if ext == ".pdf":
                with pdfplumber.open(path) as pdf:
                    text = ""
                    all_words = []
                    for page_num, page in enumerate(pdf.pages):
                        words = page.extract_words(extra_attrs=["fontname", "size", "x0", "y0"])
                        all_words.extend(words)
                        page_text = page.extract_text()
                        if page_text:
                            text += page_text + "\n"
                doc = ZenDBDoc(attr_dict = {
                    "id": f"doc_{i+start_doc_id}",
                    "doc_id": i+start_doc_id,
                    "path": path,
                    "name": Path(path).name,  # .stem
                    "text": text,
                    "words": all_words,
                    "num_pages": len(pdf.pages)
                }) 
            elif ext == ".txt" or ext == ".md":
                with open(path, "r", encoding="utf-8") as f:
                    text = f.read()
                doc = ZenDBDoc(attr_dict = {
                    "id": f"doc_{i}",
                    "doc_id": i+start_doc_id,
                    "path": path,
                    "name": Path(path).name,
                    "text": text,
                    "words": [],  # txt没有视觉特征
                    "num_pages": 1
                }) 
            else:
                print(f"    ❌ 不支持的文件类型: {ext}")
                continue
            docs.append(doc)
            print(f"    ✓ 提取 {len(doc['words'])} 个词语，{len(doc['text'])} 个字符")
        except Exception as e:
            print(f"    ❌ 处理失败: {e}")
            continue
    return docs

def load_ZenDBDoc_from_directory(docs_dir: str, table_name: str, start_doc_id = 1, debug_flag = False) -> List[ZenDBDoc]:
    docs = []
    print(f"Loading documents from {docs_dir}...")
    # doc_id = start_doc_id
    
    # 获取目录下所有.txt文件和pdf文件，然后合并
    txt_files = glob.glob(os.path.join(docs_dir, "*.txt"))
    pdf_files = glob.glob(os.path.join(docs_dir, "*.pdf")) 

    txt_files.extend(pdf_files)
    txt_files.sort()  # 确保文件顺序一致
    
    print(f"Loading documents from {docs_dir}...")
    print(f"Found {len(txt_files)} text files") 
    if debug_flag:
        print(f"    🔍 调试模式：只处理前1个文档")
        txt_files = txt_files[0:1]
    docs = util_load_zendb_docs(txt_files, debug_flag=debug_flag, topK=5, start_doc_id = start_doc_id) 
    next_doc_id = start_doc_id + len(docs) 
    
    return docs, next_doc_id




def load_TextDocs_from_directory(docs_dir: str, table_name: str, start_doc_id = 1, debug_flag = False) -> List[TextDoc]:
    """
    从指定目录加载文档，创建TextDoc对象列表
    
    Args:
        docs_dir: 文档目录路径
        table_name: 表名，用于生成doc_id
        
    Returns:
        TextDoc对象列表
    """
    docs = []
    doc_id = start_doc_id
    
    # 获取目录下所有.txt文件
    txt_files = glob.glob(os.path.join(docs_dir, "*.txt"))
    txt_files.sort()  # 确保文件顺序一致
    
    print(f"Loading documents from {docs_dir}...")
    print(f"Found {len(txt_files)} text files")
    
    for file_path in txt_files:
        if debug_flag and len(docs) >= 2:  # 限制调试时只处理前10个文档
            print(f"    🔍 调试模式：只处理前2个文档")
            break
        try:
            # 读取文件内容
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 获取文件名（包含后缀）
            file_name = os.path.basename(file_path)
            
            # 创建TextDoc对象，metadata中包含file_name
            doc = TextDoc(
                content=content,
                doc_id=doc_id,
                metadata={"file_name": file_name, "table": table_name}
            )
            
            docs.append(doc)
            doc_id += 1
            
            print(f"  Loaded: {file_name} (doc_id: {doc.doc_id})")
            
        except Exception as e:
            print(f"Error loading file {file_path}: {e}")
            continue
    
    print(f"Successfully loaded {len(docs)} documents from {table_name}")
    next_doc_id = doc_id
    return docs, next_doc_id

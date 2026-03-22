#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
一站式维基百科语料处理脚本
流程：下载bz2文件 → 解压 → 提取JSON → 转换为TXT
过程文件：corpus/processing
最终TXT：corpus/raw
"""

import os
import sys
import json
import re
import time
import requests
import bz2
import shutil
from tqdm import tqdm
import subprocess
from pathlib import Path

# ===================== 全局配置 =====================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "corpus", "raw")          # 最终TXT目录
PROCESSING_DIR = os.path.join(PROJECT_ROOT, "corpus", "processing")  # 过程文件目录
ENCODING = "utf-8"
MAX_SIZE_MB = 50                                              # 每个文件最大50M
MAX_SIZE = MAX_SIZE_MB * 1024 * 1024                          # 字节数
WIKI_URL = "https://dumps.wikimedia.org/zhwiki/latest/zhwiki-latest-pages-articles.xml.bz2"
WIKI_BZ2_FILENAME = "zhwiki_latest.xml.bz2"
WIKI_XML_FILENAME = "zhwiki_latest.xml"

# ===================== 通用工具函数 =====================
def ensure_dirs():
    """确保所有目录存在"""
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSING_DIR, exist_ok=True)
    print(f"📁 目录准备完成")
    print(f"   - 过程文件: {PROCESSING_DIR}")
    print(f"   - 最终TXT: {RAW_DIR}")

def download_file(url, save_path, min_size=1*1024*1024):
    """下载文件（带进度条、完整性检查）"""
    if os.path.exists(save_path):
        if os.path.getsize(save_path) >= min_size:
            print(f"✅ {os.path.basename(save_path)} 已存在，跳过下载")
            return True
        else:
            print(f"⚠️ {os.path.basename(save_path)} 文件残缺，重新下载")
            os.remove(save_path)

    print(f"🔽 开始下载: {os.path.basename(save_path)}")
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, stream=True, headers=headers, timeout=1200)
        resp.raise_for_status()

        total_size = int(resp.headers.get("content-length", 0))
        with open(save_path, "wb") as f, tqdm(
            total=total_size, unit="B", unit_scale=True, desc=os.path.basename(save_path)
        ) as bar:
            for chunk in resp.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

        final_size = os.path.getsize(save_path)
        if final_size < min_size:
            raise Exception(f"文件太小({final_size} bytes)，小于最小要求({min_size} bytes)")
        
        print(f"✅ 下载完成: {os.path.basename(save_path)} ({final_size/1024/1024:.1f}MB)")
        return True
    except Exception as e:
        if os.path.exists(save_path):
            os.remove(save_path)
        print(f"❌ 下载失败: {str(e)}")
        return False

def decompress_bz2(bz2_path, output_path):
    """解压bz2文件（带进度条）"""
    if os.path.exists(output_path):
        print(f"✅ {os.path.basename(output_path)} 已解压，跳过")
        return True
    
    print(f"📦 开始解压: {os.path.basename(bz2_path)}")
    try:
        total_size = os.path.getsize(bz2_path)
        with open(bz2_path, 'rb') as f_in, open(output_path, 'wb') as f_out, tqdm(
            total=total_size, unit="B", unit_scale=True, desc="解压进度"
        ) as bar:
            decompressor = bz2.BZ2Decompressor()
            while True:
                chunk = f_in.read(1024*1024)  # 1MB chunks
                if not chunk:
                    break
                data = decompressor.decompress(chunk)
                f_out.write(data)
                bar.update(len(chunk))
        
        print(f"✅ 解压完成: {os.path.basename(output_path)}")
        return True
    except Exception as e:
        if os.path.exists(output_path):
            os.remove(output_path)
        print(f"❌ 解压失败: {str(e)}")
        return False

# ===================== Wiki提取JSON函数 =====================
def extract_title(text):
    """提取页面标题"""
    match = re.search(r"<title>(.*?)</title>", text)
    return match.group(1).strip() if match else "Untitled"

def extract_text(text):
    """提取并清理页面内容"""
    match = re.search(r"<text.*?>(.*?)</text>", text, re.DOTALL)
    if not match:
        return ""
    t = match.group(1)
    # 清理维基百科标记
    t = re.sub(r"{{.*?}}", "", t)
    t = re.sub(r"\[\[.*?\]\]", "", t)
    t = re.sub(r"<.*?>", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()

def process_wiki_to_json(xml_file, output_dir, json_mode=True):
    """流式处理XML文件生成JSON（不爆内存）"""
    print(f"🔧 开始提取Wiki内容到JSON: {os.path.basename(xml_file)}")
    
    buffer = []
    file_count = 1
    max_size = MAX_SIZE  # 50M per file
    current_size = 0
    json_path = os.path.join(output_dir, f"wiki_{file_count:04d}.json")
    out = open(json_path, "w", encoding=ENCODING)

    try:
        with open(xml_file, "rb") as f:
            for line_bytes in f:
                try:
                    line = line_bytes.decode(ENCODING, errors="ignore")
                except:
                    continue

                # 捕获页面边界
                if "<page>" in line:
                    buffer = []
                elif "</page>" in line:
                    text = "".join(buffer)
                    title = extract_title(text)
                    content = extract_text(text)

                    if len(content) > 200:  # 过滤短内容
                        item = json.dumps({"title": title, "text": content}, ensure_ascii=False) + "\n"
                        item_size = len(item.encode(ENCODING))

                        # 超过大小限制则新建文件
                        if current_size + item_size > max_size:
                            out.close()
                            file_count += 1
                            json_path = os.path.join(output_dir, f"wiki_{file_count:04d}.json")
                            out = open(json_path, "w", encoding=ENCODING)
                            current_size = 0

                        out.write(item)
                        current_size += item_size
                else:
                    buffer.append(line)
        
        out.close()
        print(f"✅ JSON提取完成，生成 {file_count} 个JSON文件")
        return True
    except Exception as e:
        print(f"❌ JSON提取失败: {str(e)}")
        if out:
            out.close()
        return False

# ===================== JSON转TXT函数 =====================
def convert_json_to_txt(json_dir, output_dir):
    """将JSON文件转换为TXT文件"""
    print(f"📝 开始转换JSON到TXT，每个文件最大 {MAX_SIZE_MB}MB")
    
    file_index = 1
    current_size = 0
    out = None
    start_time = time.time()

    # 新建TXT文件函数
    def new_file():
        nonlocal out, current_size
        if out:
            out.close()
        filename = f"wiki_{file_index:02d}_{MAX_SIZE_MB}M.txt"
        path = os.path.join(output_dir, filename)
        out = open(path, "w", encoding=ENCODING)
        current_size = 0
        print(f"   新建TXT文件: {filename}")

    new_file()

    # 遍历所有JSON文件
    json_files = [f for f in sorted(os.listdir(json_dir)) if f.endswith(".json")]
    if not json_files:
        print("⚠️  未找到JSON文件")
        return False

    for fname in json_files:
        fpath = os.path.join(json_dir, fname)
        print(f"   处理JSON: {fname}")

        with open(fpath, "r", encoding=ENCODING) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    text = data.get("text", "")
                    if len(text) < 100:  # 过滤过短内容
                        continue

                    # 准备输出内容
                    output_text = text + "\n\n"
                    byte_size = len(output_text.encode(ENCODING))

                    # 超过大小限制则新建文件
                    if current_size + byte_size > MAX_SIZE:
                        file_index += 1
                        new_file()

                    out.write(output_text)
                    current_size += byte_size

                except Exception as e:
                    continue

    if out:
        out.close()

    # 统计结果
    elapsed = time.time() - start_time
    print(f"\n✅ TXT转换完成！")
    print(f"   📂 输出目录: {output_dir}")
    print(f"   ⏱  耗时: {elapsed:.2f}s")
    print(f"   📄 生成文件数: {file_index} 个")
    return True

# ===================== 主流程函数 =====================
def main():
    print("================================================")
    print("      一站式维基百科语料处理脚本")
    print("      流程：下载 → 解压 → 提取JSON → 转换TXT")
    print("================================================")
    
    # 1. 准备目录
    ensure_dirs()
    
    # 2. 下载维基百科bz2文件
    bz2_path = os.path.join(PROCESSING_DIR, WIKI_BZ2_FILENAME)
    if not download_file(WIKI_URL, bz2_path, min_size=300*1024*1024):  # 最小300MB
        sys.exit(1)
    
    # 3. 解压bz2文件
    xml_path = os.path.join(PROCESSING_DIR, WIKI_XML_FILENAME)
    if not decompress_bz2(bz2_path, xml_path):
        sys.exit(1)
    
    # 4. 提取Wiki内容到JSON
    if not process_wiki_to_json(xml_path, PROCESSING_DIR):
        sys.exit(1)
    
    # 5. 转换JSON到TXT（最终输出到raw目录）
    if not convert_json_to_txt(PROCESSING_DIR, RAW_DIR):
        sys.exit(1)
    
    # 6. 完成提示
    print("\n🎉 所有处理完成！")
    print(f"📁 最终TXT文件位置: {RAW_DIR}")
    print(f"📁 过程文件位置: {PROCESSING_DIR}（可按需删除）")

if __name__ == "__main__":
    main()

import os
import requests

# ======================
# 项目统一根目录
# ======================
PROJECT_ROOT = "../"
RAW_DIR = os.path.join(PROJECT_ROOT, "corpus", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

def download_wikitext2():
    url = "https://s3.amazonaws.com/research.metamind.io/wikitext/wikitext-2-raw-v1.zip"
    save_path = os.path.join(RAW_DIR, "wikitext-2-raw.zip")

    if os.path.exists(save_path):
        print("✅ WikiText-2 已存在，跳过下载")
        return

    print("🔽 开始下载英文开源语料 WikiText-2...")
    resp = requests.get(url, stream=True)
    with open(save_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
    print("✅ 英文语料下载完成")

def generate_safe_chinese_corpus():
    # 原创无版权 + 开源风格中文语料
    text = """
人工智能是计算机科学的重要分支。
大模型依靠高质量数据实现能力进化。
数据工程是大模型训练的基础核心环节。
分词器将文本转为模型可接受的输入格式。
BPE 是现代大模型最常用的子词分词算法。
本项目为个人练习，使用开源与原创语料。
数据质量直接决定大模型的最终上限。
合规安全是数据工程必须遵守的第一原则。
"""
    save_path = os.path.join(RAW_DIR, "chinese_open_safe.txt")
    with open(save_path, "w", encoding="utf-8") as f:
        f.write(text.strip())
    print("✅ 安全中文语料生成完成")

if __name__ == "__main__":
    print("================================================")
    print("      数据工程实战01：开源无版权语料获取")
    print("      项目根目录：data_engineering")
    print("================================================")
    download_wikitext2()
    generate_safe_chinese_corpus()
    print("\n🎉 全部语料准备完成！")

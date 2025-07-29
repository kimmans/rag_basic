from dotenv import load_dotenv
load_dotenv()

import nest_asyncio
nest_asyncio.apply()

# 환경변수 확인
import os

from llama_cloud_services import LlamaParse
import json

parser = LlamaParse(
    result_type="markdown",  # "markdown" 또는 "text" 선택 가능
    language="ko",           # 언어 설정
    parse_mode="parse_page_with_lvm",  # 파싱 모드 선택
    # 추가 옵션:
    # disable_image_extraction=False,  # 이미지 추출 비활성화 옵션
    # disable_ocr=False,               # OCR 비활성화 옵션
    vendor_multimodal_model_name="openai-gpt4o"  # LVM 모델을 Gemini로 변경
)

# 파싱할 문서 경로 설정
paper_path = "data/CVM_articles.pdf"
data_dir = "data"

print(f"📄 PDF 파일 파싱 시작: {paper_path}")

# 문서 파싱 및 결과 가져오기
md_json_objs = parser.get_json_result(paper_path)
json_dicts = md_json_objs[0]["pages"]

print("✅ PDF 파싱 완료!")

# 결과 확인 및 출력
if json_dicts:
    print(f"\n📊 파싱 결과:")
    print(f"  - 총 페이지 수: {len(json_dicts)}")
    
    # 첫 번째 페이지 결과 확인
    first_page = json_dicts[0]
    print(f"\n📄 첫 번째 페이지 정보:")
    print(f"  - 페이지 번호: {first_page.get('page_number', 'N/A')}")
    
    # 마크다운 텍스트 미리보기
    if "md" in first_page:
        md_text = first_page["md"]
        print(f"  - 마크다운 길이: {len(md_text)} 문자")
        preview_text = md_text[:500] + "..." if len(md_text) > 500 else md_text
        print(f"  - 마크다운 미리보기:")
        print("=" * 60)
        print(preview_text)
        print("=" * 60)
    
    # 블록 정보
    blocks = first_page.get('blocks', [])
    if blocks:
        print(f"\n📋 블록 정보 (처음 3개):")
        for i, block in enumerate(blocks[:3]):
            print(f"  블록 {i+1}: {block.get('type', 'unknown')} - {block.get('text', '')[:100]}...")
    
    # JSON 결과 저장
    with open("parsed_result.json", "w", encoding="utf-8") as f:
        json.dump(md_json_objs, f, ensure_ascii=False, indent=2)
    print(f"\n💾 JSON 결과가 'parsed_result.json' 파일에 저장되었습니다!")
    
    # 마크다운 결과 저장
    markdown_content = ""
    for page in json_dicts:
        if "md" in page:
            page_num = page.get('page', 'N/A')
            markdown_content += f"# 페이지 {page_num}\n\n"
            markdown_content += page["md"] + "\n\n"
            markdown_content += "---\n\n"
    
    with open("parsed_result.md", "w", encoding="utf-8") as f:
        f.write(markdown_content)
    print(f"💾 마크다운 결과가 'parsed_result.md' 파일에 저장되었습니다!")
    
    # 통계
    total_text = ""
    for page in json_dicts:
        if "md" in page:
            total_text += page["md"] + "\n"
    
    print(f"\n📊 통계:")
    print(f"  - 총 페이지 수: {len(json_dicts)}")
    print(f"  - 총 텍스트 길이: {len(total_text)} 문자")
    print(f"  - 단어 수: {len(total_text.split())}")
    
else:
    print("❌ 파싱 결과가 비어있습니다.")
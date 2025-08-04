from dotenv import load_dotenv
load_dotenv()

import nest_asyncio
nest_asyncio.apply()

# 환경변수 확인
import os
import glob
from pathlib import Path
import time
import random

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
data_dir = "data"
output_dir = "data/parsed"

# 출력 디렉토리 생성
Path(output_dir).mkdir(parents=True, exist_ok=True)

# data 폴더에서 모든 PDF 파일 찾기
pdf_files = glob.glob(f"{data_dir}/*.pdf")

print(f"📄 발견된 PDF 파일 수: {len(pdf_files)}")

# 각 PDF 파일 처리
for i, pdf_path in enumerate(pdf_files, 1):
    pdf_name = Path(pdf_path).stem  # 파일명에서 확장자 제거
    
    print(f"\n🔄 [{i}/{len(pdf_files)}] PDF 파일 파싱 시작: {pdf_name}")
    
    # 이미 처리된 파일인지 확인
    json_output_path = f"{output_dir}/{pdf_name}_parsed.json"
    if os.path.exists(json_output_path):
        print(f"⏭️ {pdf_name}은 이미 처리되었습니다. 건너뜁니다.")
        continue
    
    try:
        # 요청 간 지연시간 추가 (1-3초 랜덤)
        if i > 1:  # 첫 번째 파일이 아닌 경우에만 지연
            delay = random.uniform(1, 3)
            print(f"⏳ {delay:.1f}초 대기 중...")
            time.sleep(delay)
        
        # 문서 파싱 및 결과 가져오기
        md_json_objs = parser.get_json_result(pdf_path)
        json_dicts = md_json_objs[0]["pages"]
        
        print(f"✅ {pdf_name} 파싱 완료!")
        
        # 결과 확인 및 출력
        if json_dicts:
            print(f"  📊 파싱 결과:")
            print(f"    - 총 페이지 수: {len(json_dicts)}")
            
            # 첫 번째 페이지 결과 확인
            first_page = json_dicts[0]
            print(f"    - 페이지 번호: {first_page.get('page_number', 'N/A')}")
            
            # 마크다운 텍스트 미리보기
            if "md" in first_page:
                md_text = first_page["md"]
                print(f"    - 마크다운 길이: {len(md_text)} 문자")
                preview_text = md_text[:200] + "..." if len(md_text) > 200 else md_text
                print(f"    - 마크다운 미리보기: {preview_text}")
            
            # JSON 결과 저장
            with open(json_output_path, "w", encoding="utf-8") as f:
                json.dump(md_json_objs, f, ensure_ascii=False, indent=2)
            print(f"    💾 JSON 결과 저장: {json_output_path}")
            
            # 마크다운 결과 저장
            markdown_content = f"# {pdf_name}\n\n"
            for page in json_dicts:
                if "md" in page:
                    page_num = page.get('page_number', 'N/A')
                    markdown_content += f"## 페이지 {page_num}\n\n"
                    markdown_content += page["md"] + "\n\n"
                    markdown_content += "---\n\n"
            
            md_output_path = f"{output_dir}/{pdf_name}_parsed.md"
            with open(md_output_path, "w", encoding="utf-8") as f:
                f.write(markdown_content)
            print(f"    💾 마크다운 결과 저장: {md_output_path}")
            
            # 통계
            total_text = ""
            for page in json_dicts:
                if "md" in page:
                    total_text += page["md"] + "\n"
            
            print(f"    📊 통계:")
            print(f"      - 총 페이지 수: {len(json_dicts)}")
            print(f"      - 총 텍스트 길이: {len(total_text)} 문자")
            print(f"      - 단어 수: {len(total_text.split())}")
            
        else:
            print(f"❌ {pdf_name} 파싱 결과가 비어있습니다.")
            
    except Exception as e:
        print(f"❌ {pdf_name} 파싱 중 오류 발생: {str(e)}")
        
        # 429 에러인 경우 더 긴 대기시간
        if "429" in str(e) or "Too Many Requests" in str(e):
            print(f" Rate limit 도달. 30초 대기 후 재시도...")
            time.sleep(30)
            try:
                md_json_objs = parser.get_json_result(pdf_path)
                json_dicts = md_json_objs[0]["pages"]
                print(f"✅ {pdf_name} 재시도 성공!")
                # 성공한 경우 결과 저장 로직 반복...
                # (위의 저장 로직과 동일)
            except Exception as retry_e:
                print(f"❌ {pdf_name} 재시도 실패: {str(retry_e)}")
        
        continue

print(f"\n🎉 모든 PDF 파일 처리 완료!")
print(f"📁 결과 파일들은 '{output_dir}' 폴더에 저장되었습니다.")
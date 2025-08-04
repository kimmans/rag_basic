from dotenv import load_dotenv
load_dotenv()

import nest_asyncio
nest_asyncio.apply()

# 환경변수 확인
import os
import glob
from pathlib import Path
import time
import base64
from PIL import Image
from io import BytesIO
import requests
import json

# Docling 관련 import
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import PdfFormatOption

def extract_images_from_pdf(pdf_path, output_dir):
    """PDF에서 이미지를 추출하여 폴더에 저장"""
    
    print(f"   🖼️ 이미지 추출 중...")
    
    # 이미지 추출을 위한 파이프라인 옵션 설정
    pipeline_options = PdfPipelineOptions()
    pipeline_options.generate_picture_images = True
    pipeline_options.generate_page_images = True
    pipeline_options.do_table_structure = True
    pipeline_options.images_scale = 2.0
    
    # 변환기 설정
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )
    
    # PDF 변환
    result = converter.convert(pdf_path)
    
    # 이미지 저장 폴더 생성 (parents=True로 중첩 디렉토리 생성)
    images_dir = output_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)
    
    saved_images = []
    
    # 이미지 추출 및 저장
    for idx, picture in enumerate(result.document.pictures):
        try:
            # base64 데이터 추출
            uri_str = str(picture.image.uri)
            if ',' in uri_str:
                base64_data = uri_str.split(',')[1]
                
                # 디코딩 및 PIL 변환
                image_data = base64.b64decode(base64_data)
                image = Image.open(BytesIO(image_data))
                
                # 페이지 번호 기반 파일 이름
                page_no = picture.prov[0].page_no if picture.prov else 0
                filename = f"page_{page_no}_img_{idx+1}.png"
                filepath = images_dir / filename
                
                image.save(filepath)
                saved_images.append({
                    'filepath': str(filepath),
                    'page_no': page_no,
                    'index': idx + 1
                })
                print(f"      ✅ 이미지 저장: {filename}")
                
        except Exception as e:
            print(f"      ❌ 이미지 저장 실패 {idx}: {e}")
    
    return saved_images

def generate_image_caption_with_vlm(image_path, openai_api_key):
    """OpenAI GPT-4o를 사용하여 이미지 캡션 생성"""
    
    try:
        # 이미지를 base64로 인코딩
        with open(image_path, "rb") as image_file:
            image_data = base64.b64encode(image_file.read()).decode('utf-8')
        
        # OpenAI API 요청
        headers = {
            "Authorization": f"Bearer {openai_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "이 이미지를 자세히 설명해주세요. 문서의 일부라면 어떤 내용인지, 표나 그래프라면 어떤 정보를 보여주는지 설명해주세요."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{image_data}"
                            }
                        }
                    ]
                }
            ],
            "max_tokens": 300
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            caption = result['choices'][0]['message']['content']
            return caption.strip()
        else:
            print(f"      ❌ VLM API 오류: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"      ❌ VLM 캡션 생성 실패: {e}")
        return None

def process_pdf_files():
    """data 폴더의 모든 PDF 파일을 처리하여 마크다운으로 변환하고 이미지 캡션 생성"""
    
    # OpenAI API 키 확인
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("❌ OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    # data 폴더에서 모든 PDF 파일 찾기
    pdf_files = glob.glob("data/*.pdf")
    
    if not pdf_files:
        print("❌ data 폴더에서 PDF 파일을 찾을 수 없습니다.")
        return
    
    print(f"📄 총 {len(pdf_files)}개의 PDF 파일을 처리합니다...")
    
    # 출력 폴더 생성
    output_dir = Path("data/parsed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 기본 변환기 (마크다운 변환용)
    converter = DocumentConverter()
    
    successful_count = 0
    failed_count = 0
    
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            # 파일명에서 확장자 제거하여 출력 파일명 생성
            pdf_name = Path(pdf_path).stem
            pdf_output_dir = output_dir / pdf_name
            
            print(f"\n[{i}/{len(pdf_files)}] 처리 중: {pdf_name}")
            print(f"   📖 읽는 중: {pdf_path}")
            
            # 변환 시작 시간 기록
            start_time = time.time()
            
            # 1. PDF를 마크다운으로 변환
            result = converter.convert(pdf_path)
            result_document = result.document.export_to_markdown()
            
            # 2. 이미지 추출 (PDF별 디렉토리 생성)
            saved_images = extract_images_from_pdf(pdf_path, pdf_output_dir)
            
            # 3. 이미지 캡션 생성
            image_captions = []
            if saved_images:
                print(f"   🤖 {len(saved_images)}개 이미지에 대한 캡션 생성 중...")
                
                for img_info in saved_images:
                    print(f"      🖼️ 이미지 처리 중: {Path(img_info['filepath']).name}")
                    
                    caption = generate_image_caption_with_vlm(
                        img_info['filepath'], 
                        openai_api_key
                    )
                    
                    if caption:
                        image_captions.append({
                            'page': img_info['page_no'],
                            'image': Path(img_info['filepath']).name,
                            'caption': caption
                        })
                        print(f"      ✅ 캡션 생성 완료")
                    else:
                        print(f"      ❌ 캡션 생성 실패")
            
            # 4. 마크다운에 이미지 캡션 추가
            if image_captions:
                result_document += "\n\n## 이미지 캡션\n\n"
                for caption_info in image_captions:
                    result_document += f"### 페이지 {caption_info['page']} - {caption_info['image']}\n\n"
                    result_document += f"{caption_info['caption']}\n\n"
                    result_document += "---\n\n"
            
            # 5. 결과를 마크다운 파일로 저장
            output_path = output_dir / f"{pdf_name}.md"
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(result_document)
            
            # 6. 이미지 캡션 정보를 JSON으로도 저장
            if image_captions:
                captions_path = output_dir / f"{pdf_name}_captions.json"
                with open(captions_path, 'w', encoding='utf-8') as f:
                    json.dump(image_captions, f, ensure_ascii=False, indent=2)
                print(f"   💾 캡션 정보 저장: {captions_path}")
            
            # 처리 시간 계산
            elapsed_time = time.time() - start_time
            
            print(f"   ✅ 완료: {output_path} ({elapsed_time:.2f}초)")
            print(f"    통계: 이미지 {len(saved_images)}개, 캡션 {len(image_captions)}개")
            successful_count += 1
            
        except Exception as e:
            print(f"   ❌ 실패: {pdf_name} - {str(e)}")
            failed_count += 1
    
    # 최종 결과 출력
    print(f"\n🎉 모든 처리 완료!")
    print(f"   성공: {successful_count}개")
    print(f"   실패: {failed_count}개")
    print(f"   결과 저장 위치: {output_dir}")

if __name__ == "__main__":
    process_pdf_files()


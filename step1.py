import logging
import time
from pathlib import Path
from docling_core.types.doc import ImageRefMode, PictureItem, TableItem
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
import glob
import os
import re

def setup_logging():
    """로깅 설정"""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)

def process_pdf_files():
    """data 폴더의 모든 PDF 파일을 처리하여 마크다운으로 변환"""
    
    _log = setup_logging()
    
    # 입력 디렉토리와 출력 디렉토리 설정
    input_dir = Path("data")
    output_dir = Path("data/parsed")
    
    # 출력 디렉토리 생성
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # PDF 파일들 찾기
    pdf_files = list(input_dir.glob("*.pdf"))
    
    if not pdf_files:
        print("❌ data 폴더에서 PDF 파일을 찾을 수 없습니다.")
        return
    
    print(f" 총 {len(pdf_files)}개의 PDF 파일을 처리합니다...")
    
    # PDF 파이프라인 옵션 구성 (더 간단한 설정)
    pipeline_options = PdfPipelineOptions()
    pipeline_options.images_scale = 2.0
    pipeline_options.generate_picture_images = True
    pipeline_options.do_formula_enrichment = False  # 수식 처리 비활성화
    pipeline_options.do_picture_classification = False  # 이미지 분류 비활성화
    pipeline_options.do_table_structure = True
    
    # 문서 변환기 초기화
    doc_converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )
    
    successful_count = 0
    failed_count = 0
    
    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            pdf_name = pdf_path.stem
            print(f"\n[{i}/{len(pdf_files)}] 처리 중: {pdf_name}")
            print(f"   📖 읽는 중: {pdf_path}")
            
            # 변환 시작 시간 기록
            start_time = time.time()
            
            # PDF 문서 변환 실행
            conv_res = doc_converter.convert(pdf_path)
            
            # 문서 구조 분석
            element_types = set()
            for element, _level in conv_res.document.iterate_items():
                element_types.add(type(element).__name__)
            
            print(f"   📊 문서 구조:")
            for element_type in sorted(element_types):
                print(f"      - {element_type}")
            
            # 각 PDF별 출력 디렉토리 생성
            pdf_output_dir = output_dir / pdf_name
            pdf_output_dir.mkdir(exist_ok=True)
            
            # 이미지 참조가 포함된 마크다운 파일 저장
            md_filename = pdf_output_dir / f"{pdf_name}-with-image-refs.md"
            conv_res.document.save_as_markdown(md_filename, image_mode=ImageRefMode.REFERENCED)
            
            # 변환 완료 시간 계산
            end_time = time.time() - start_time
            
            print(f"   ✅ 완료: {md_filename} ({end_time:.2f}초)")
            successful_count += 1
            
        except Exception as e:
            print(f"   ❌ 실패: {pdf_name} - {str(e)}")
            failed_count += 1
    
    # 최종 결과 출력
    print(f"\n🎉 모든 처리 완료!")
    print(f"   성공: {successful_count}개")
    print(f"   실패: {failed_count}개")
    print(f"   결과 저장 위치: {output_dir}")
    
    return successful_count, failed_count

def analyze_document_structure():
    """처리된 문서들의 구조를 분석"""
    
    parsed_dir = Path("data/parsed")
    
    if not parsed_dir.exists():
        print("❌ parsed 디렉토리가 존재하지 않습니다.")
        return
    
    print(f"\n📊 문서 구조 분석")
    print("=" * 50)
    
    # 각 PDF별 디렉토리 순회
    for pdf_dir in parsed_dir.glob("*"):
        if pdf_dir.is_dir():
            pdf_name = pdf_dir.name
            md_file = pdf_dir / f"{pdf_name}-with-image-refs.md"
            
            if md_file.exists():
                # 파일 크기 확인
                file_size = md_file.stat().st_size
                
                # 이미지 파일들 확인
                images_dir = pdf_dir / f"{pdf_name}-with-image-refs_artifacts"
                image_count = 0
                if images_dir.exists():
                    image_count = len(list(images_dir.glob("*.png")))
                
                print(f"📄 {pdf_name}")
                print(f"   - 마크다운 파일: {file_size:,} bytes")
                print(f"   - 이미지 개수: {image_count}개")
                
                # 마크다운 내용 미리보기
                try:
                    with open(md_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # 이미지 참조 개수 확인
                    image_refs = content.count('![')
                    print(f"   - 이미지 참조: {image_refs}개")
                    
                    # 첫 번째 이미지 참조 찾기
                    import re
                    image_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
                    matches = re.findall(image_pattern, content)
                    
                    if matches:
                        print(f"   - 첫 번째 이미지: {matches[0][1]}")
                    
                except Exception as e:
                    print(f"   - 파일 읽기 오류: {e}")
                
                print()

def create_image_captions_with_gemini():
    """Gemini를 사용하여 이미지 캡션 생성"""
    
    # 환경변수 로드
    from dotenv import load_dotenv
    load_dotenv()
    
    # Gemini 모델 초기화
    from langchain_google_genai import ChatGoogleGenerativeAI
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash-preview-05-20")
    
    parsed_dir = Path("data/parsed")
    
    if not parsed_dir.exists():
        print("❌ parsed 디렉토리가 존재하지 않습니다.")
        return
    
    print(f"\n️ Gemini를 사용한 이미지 캡션 생성")
    print("=" * 50)
    
    for pdf_dir in parsed_dir.glob("*"):
        if pdf_dir.is_dir():
            pdf_name = pdf_dir.name
            md_file = pdf_dir / f"{pdf_name}-with-image-refs.md"
            images_dir = pdf_dir / f"{pdf_name}-with-image-refs_artifacts"
            
            if md_file.exists() and images_dir.exists():
                print(f"\n📄 {pdf_name} 이미지 캡션 생성 중...")
                
                # 마크다운 파일 읽기
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # 이미지 참조 패턴 찾기
                image_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
                matches = re.findall(image_pattern, content)
                
                print(f"   발견된 이미지 참조: {len(matches)}개")
                
                # 각 이미지에 대해 캡션 생성
                for i, (alt_text, image_path) in enumerate(matches, 1):
                    try:
                        # 이미지 파일 경로
                        full_image_path = pdf_dir / image_path
                        
                        if full_image_path.exists():
                            print(f"   [{i}/{len(matches)}] 처리 중: {image_path}")
                            
                            # 이미지를 base64로 인코딩
                            with open(full_image_path, "rb") as image_file:
                                encoded_image = base64.b64encode(image_file.read()).decode("utf-8")
                            
                            # Gemini에 전송할 메시지 생성
                            from langchain_core.messages import HumanMessage
                            message = HumanMessage(
                                content=[
                                    {"type": "text", "text": "이 이미지를 텍스트로 대체하고자 합니다. 해당 이미지에 대한 설명을 한글로 생성해주세요."},
                                    {"type": "image_url", "image_url": f"data:image/png;base64,{encoded_image}"},
                                ]
                            )
                            
                            # 캡션 생성
                            result = llm.invoke([message])
                            caption = result.content.strip()
                            
                            print(f"   ✅ 캡션 생성 완료: {caption[:100]}...")
                            
                            # 마크다운에서 이미지 참조를 캡션으로 대체
                            image_ref = f"![{alt_text}]({image_path})"
                            content = content.replace(image_ref, f"[이미지 캡션: {caption}]")
                            
                        else:
                            print(f"   ❌ 이미지 파일을 찾을 수 없음: {full_image_path}")
                            
                    except Exception as e:
                        print(f"   ❌ 캡션 생성 실패: {e}")
                
                # 캡션이 적용된 새로운 마크다운 파일 저장
                captioned_md_file = pdf_dir / f"{pdf_name}-with-captions.md"
                with open(captioned_md_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"   💾 캡션이 적용된 파일 저장: {captioned_md_file}")

def main():
    """메인 실행 함수"""
    
    print("🚀 PDF 문서 변환 및 처리 시작")
    print("=" * 50)
    
    # 1단계: PDF 파일들을 마크다운으로 변환
    print("\n📄 1단계: PDF 파일 변환")
    successful, failed = process_pdf_files()
    
    if successful > 0:
        # 2단계: 문서 구조 분석
        print("\n📊 2단계: 문서 구조 분석")
        analyze_document_structure()
        
        # 3단계: Gemini를 사용한 이미지 캡션 생성
        print("\n️ 3단계: 이미지 캡션 생성")
        create_image_captions_with_gemini()
        
        print(f"\n✅ 단계 1 완료!")
        print(f"   다음 단계를 위해 다음 파일들을 확인하세요:")
        print(f"   - data/parsed/ 폴더의 마크다운 파일들")
        print(f"   - 각 PDF별 이미지 파일들")
        print(f"   - 캡션이 적용된 마크다운 파일들")
        
    else:
        print(f"\n❌ 처리된 파일이 없습니다.")

if __name__ == "__main__":
    main()
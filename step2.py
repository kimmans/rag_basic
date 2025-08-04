import logging
import time
from pathlib import Path
import json
import base64
from PIL import Image
import voyageai
from qdrant_client import QdrantClient
from qdrant_client.http import models
import uuid
import re
from dotenv import load_dotenv
import os

def setup_logging():
    """로깅 설정"""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)

def preprocess_korean_text(text):
    """한글 텍스트 전처리"""
    # 불필요한 공백 제거
    text = re.sub(r'\s+', ' ', text.strip())
    # 특수문자 정리
    text = re.sub(r'[^\w\s가-힣]', ' ', text)
    # 연속된 공백을 하나로
    text = re.sub(r'\s+', ' ', text)
    return text

def chunk_text(text, max_length=1000):
    """텍스트를 적절한 크기로 청킹"""
    if len(text) <= max_length:
        return [text]
    
    chunks = []
    sentences = re.split(r'[.!?。！？]', text)
    current_chunk = ""
    
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
            
        if len(current_chunk) + len(sentence) < max_length:
            current_chunk += sentence + ". "
        else:
            if current_chunk:
                chunks.append(current_chunk.strip())
            current_chunk = sentence + ". "
    
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks if chunks else [text]

def load_processed_data():
    """step1에서 처리된 데이터를 로드"""
    
    parsed_dir = Path("data/parsed")
    
    if not parsed_dir.exists():
        print("❌ parsed 디렉토리가 존재하지 않습니다. step1.py를 먼저 실행하세요.")
        return []
    
    processed_files = []
    
    # 각 PDF별 디렉토리 순회
    for pdf_dir in parsed_dir.glob("*"):
        if pdf_dir.is_dir():
            pdf_name = pdf_dir.name
            
            # 마크다운 파일들 확인
            md_with_refs = pdf_dir / f"{pdf_name}-with-image-refs.md"
            md_with_captions = pdf_dir / f"{pdf_name}-with-captions.md"
            images_dir = pdf_dir / f"{pdf_name}-with-image-refs_artifacts"
            
            if md_with_refs.exists() and images_dir.exists():
                # 마크다운 내용 로드
                with open(md_with_refs, 'r', encoding='utf-8') as f:
                    md_content = f.read()
                
                # 이미지 파일들 확인
                image_files = list(images_dir.glob("*.png"))
                
                # 캡션이 적용된 마크다운이 있으면 사용
                if md_with_captions.exists():
                    with open(md_with_captions, 'r', encoding='utf-8') as f:
                        captioned_content = f.read()
                else:
                    captioned_content = md_content
                
                processed_files.append({
                    'pdf_name': pdf_name,
                    'md_content': md_content,
                    'captioned_content': captioned_content,
                    'image_files': image_files,
                    'images_dir': images_dir
                })
                
                print(f"✅ {pdf_name} 데이터 로드 완료")
                print(f"   - 마크다운: {len(md_content)} 문자")
                print(f"   - 이미지: {len(image_files)}개")
    
    return processed_files

def create_multimodal_sequences(processed_data):
    """마크다운 텍스트와 이미지를 인터리브하여 멀티모달 시퀀스 생성"""
    
    sequences = []
    
    for pdf_data in processed_data:
        pdf_name = pdf_data['pdf_name']
        md_content = pdf_data['md_content']
        captioned_content = pdf_data['captioned_content']
        image_files = pdf_data['image_files']
        images_dir = pdf_data['images_dir']
        
        print(f"\n {pdf_name} 멀티모달 시퀀스 생성 중...")
        
        # 마크다운에서 이미지 참조 위치 찾기
        image_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
        matches = list(re.finditer(image_pattern, md_content))
        
        if matches:
            # 이미지 참조가 있는 경우, 텍스트를 분할하여 인터리브 시퀀스 생성
            current_pos = 0
            multimodal_content = []
            
            for i, match in enumerate(matches):
                # 이미지 참조 이전 텍스트 추가
                text_before = md_content[current_pos:match.start()].strip()
                if text_before:
                    multimodal_content.append({"type": "text", "content": text_before})
                
                # 이미지 추가
                if i < len(image_files):
                    image_file = image_files[i]
                    image_name = image_file.name
                    
                    try:
                        # 이미지를 base64로 인코딩
                        with open(image_file, "rb") as img_file:
                            img_data = base64.b64encode(img_file.read()).decode('utf-8')
                        
                        # 캡션된 콘텐츠에서 해당 이미지 부분의 캡션 찾기
                        caption = ""
                        caption_pattern = r'\[이미지 캡션: ([^\]]+)\]'
                        caption_matches = re.findall(caption_pattern, captioned_content)
                        if i < len(caption_matches):
                            caption = caption_matches[i]
                        
                        multimodal_content.append({
                            "type": "image", 
                            "content": f"data:image/png;base64,{img_data}",
                            "image_name": image_name,
                            "caption": caption
                        })
                        print(f"   ✅ 이미지 추가: {image_name}")
                        
                    except Exception as e:
                        print(f"   ❌ 이미지 로드 실패 {image_name}: {e}")
                
                current_pos = match.end()
            
            # 마지막 이미지 참조 이후 텍스트 추가
            text_after = md_content[current_pos:].strip()
            if text_after:
                multimodal_content.append({"type": "text", "content": text_after})
            
            # 시퀀스 생성
            sequence = {
                'pdf_name': pdf_name,
                'multimodal_content': multimodal_content,
                'image_files': image_files,
                'images_dir': images_dir
            }
            
            sequences.append(sequence)
            print(f"   ✅ 멀티모달 시퀀스 생성 완료: {len(multimodal_content)}개 요소")
            
        else:
            # 이미지 참조가 없는 경우 텍스트만 처리
            sequence = {
                'pdf_name': pdf_name,
                'multimodal_content': [{"type": "text", "content": md_content}],
                'image_files': image_files,
                'images_dir': images_dir
            }
            sequences.append(sequence)
            print(f"   ⚠️ 텍스트만 처리: 이미지 참조 없음")
    
    return sequences

def create_voyage_multimodal_embeddings(sequences, voyage_client):
    """Voyage AI를 사용하여 멀티모달 임베딩 생성"""
    
    print(f"\n🔍 Voyage AI 멀티모달 임베딩 생성 중...")
    
    # 지원되는 모델 확인
    try:
        models_response = voyage_client.list_models()
        print(f"   지원되는 모델: {[model.id for model in models_response.models]}")
    except Exception as e:
        print(f"   모델 목록 조회 실패: {e}")
    
    embeddings = []
    
    for i, sequence in enumerate(sequences):
        try:
            print(f"   [{i+1}/{len(sequences)}] 임베딩 생성: {sequence['pdf_name']}")
            
            # 텍스트 콘텐츠만 추출하여 임베딩 (멀티모달 API 문제로 인해 텍스트 기반으로 처리)
            text_content = []
            for item in sequence['multimodal_content']:
                if item['type'] == 'text':
                    # 한글 텍스트 전처리
                    processed_text = preprocess_korean_text(item['content'])
                    if processed_text:
                        text_content.append(processed_text)
                elif item['type'] == 'image':
                    # 이미지 캡션이 있으면 추가
                    if item.get('caption'):
                        caption = preprocess_korean_text(item['caption'])
                        text_content.append(f"[이미지: {caption}]")
                    else:
                        text_content.append("[이미지]")
            
            # 텍스트를 하나의 문자열로 결합
            combined_text = " ".join(text_content)
            
            # 텍스트가 너무 길면 청킹
            text_chunks = chunk_text(combined_text, max_length=1500)
            
            print(f"   텍스트 청킹: {len(text_chunks)}개 청크")
            
            # 각 청크별로 임베딩 생성
            chunk_embeddings = []
            for j, chunk in enumerate(text_chunks):
                try:
                    # 한글 지원이 더 좋은 모델 우선 사용
                    models_to_try = ["voyage-large-2", "voyage-02", "voyage-01"]
                    
                    for model_name in models_to_try:
                        try:
                            result = voyage_client.embed(
                                texts=[chunk],
                                model=model_name
                            )
                            chunk_embeddings.append({
                                'chunk_index': j,
                                'text': chunk,
                                'embedding': result.embeddings[0],
                                'model_used': model_name
                            })
                            print(f"   ✅ 청크 {j+1} 임베딩 완료 (모델: {model_name})")
                            break
                        except Exception as e:
                            print(f"   ❌ 모델 {model_name} 실패: {e}")
                            continue
                    else:
                        print(f"   ❌ 모든 모델 실패")
                        
                except Exception as e:
                    print(f"   ❌ 청크 {j+1} 임베딩 실패: {e}")
            
            if chunk_embeddings:
                # 모든 청크의 임베딩을 평균하여 하나의 임베딩으로 결합
                import numpy as np
                avg_embedding = np.mean([chunk['embedding'] for chunk in chunk_embeddings], axis=0)
                
                embeddings.append({
                    'pdf_name': sequence['pdf_name'],
                    'multimodal_content': sequence['multimodal_content'],
                    'embedding': avg_embedding.tolist(),
                    'text': combined_text,
                    'chunks': chunk_embeddings
                })
                
                print(f"   ✅ 임베딩 생성 완료 (청크 수: {len(chunk_embeddings)})")
            else:
                print(f"   ❌ 임베딩 생성 실패: 모든 청크 실패")
            
        except Exception as e:
            print(f"   ❌ 임베딩 생성 실패: {e}")
    
    return embeddings

def save_to_qdrant(embeddings, collection_name="voyage-multimodal-docs"):
    """Qdrant에 임베딩 저장"""
    
    print(f"\n Qdrant에 저장 중...")
    
    # 로컬 Qdrant 클라이언트 생성
    qdrant_client = QdrantClient("http://localhost:6333")
    
    # 기존 컬렉션이 있으면 삭제
    try:
        qdrant_client.delete_collection(collection_name=collection_name)
        print(f"   기존 컬렉션 삭제: {collection_name}")
    except:
        pass
    
    # 첫 번째 임베딩의 차원 확인
    if embeddings:
        vector_size = len(embeddings[0]['embedding'])
        print(f"   벡터 차원: {vector_size}")
    else:
        vector_size = 1536  # voyage-large-2 기본 차원
    
    # 컬렉션 생성
    qdrant_client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=vector_size,  # 실제 임베딩 차원 사용
            distance=models.Distance.COSINE
        ),
        on_disk_payload=True
    )
    
    print(f"   컬렉션 생성: {collection_name} (차원: {vector_size})")
    
    # 포인트 생성
    points = []
    for i, emb_data in enumerate(embeddings):
        # 멀티모달 콘텐츠에서 텍스트와 이미지 정보 추출
        text_content = []
        image_info = []
        
        for item in emb_data['multimodal_content']:
            if item['type'] == 'text':
                text_content.append(item['content'])
            elif item['type'] == 'image':
                image_info.append({
                    'image_name': item.get('image_name', ''),
                    'caption': item.get('caption', ''),
                    'image_path': item.get('content', '')[:100] + '...' if len(item.get('content', '')) > 100 else item.get('content', '')
                })
                text_content.append(f"[이미지: {item.get('caption', '')}]")
        
        combined_text = " ".join(text_content)
        
        point = models.PointStruct(
            id=str(uuid.uuid4()),
            vector=emb_data['embedding'],
            payload={
                'pdf_name': emb_data['pdf_name'],
                'text': combined_text,
                'content_count': len(emb_data['multimodal_content']),
                'image_count': len(image_info),
                'images': image_info
            }
        )
        points.append(point)
    
    # Qdrant에 업로드
    qdrant_client.upsert(collection_name=collection_name, points=points)
    
    print(f"✅ {len(points)}개의 임베딩을 Qdrant에 저장 완료")
    print(f"   대시보드: http://localhost:6333/dashboard#/collections/{collection_name}")
    
    return qdrant_client, collection_name

def search_similar_documents(qdrant_client, collection_name, query_text, query_image=None, limit=5):
    """유사한 문서 검색"""
    
    print(f"\n🔍 유사 문서 검색 중...")
    
    # 쿼리 텍스트 전처리
    processed_query = preprocess_korean_text(query_text)
    print(f"   쿼리: '{query_text}' -> '{processed_query}'")
    
    # Voyage 클라이언트 생성
    voyage_client = voyageai.Client()
    
    try:
        # 한글 지원이 더 좋은 모델 우선 사용
        models_to_try = ["voyage-large-2", "voyage-02", "voyage-01"]
        query_vector = None
        
        for model_name in models_to_try:
            try:
                result = voyage_client.embed(
                    texts=[processed_query],
                    model=model_name
                )
                query_vector = result.embeddings[0]
                print(f"   쿼리 임베딩 완료 (모델: {model_name})")
                break
            except Exception as e:
                print(f"   모델 {model_name} 실패: {e}")
                continue
        
        if query_vector is None:
            print(f"   ❌ 모든 모델에서 쿼리 임베딩 생성 실패")
            return []
            
    except Exception as e:
        print(f"   ❌ 쿼리 임베딩 생성 실패: {e}")
        return []
    
    # Qdrant에서 검색
    search_result = qdrant_client.search(
        collection_name=collection_name,
        query_vector=query_vector,
        limit=limit
    )
    
    print(f"검색 결과:")
    for rank, hit in enumerate(search_result):
        print(f"{rank+1}위: {hit.payload['pdf_name']}")
        print(f"   유사도: {hit.score:.4f}")
        print(f"   콘텐츠 수: {hit.payload['content_count']}")
        print(f"   이미지 수: {hit.payload.get('image_count', 0)}")
        
        # 텍스트 미리보기 (한글 깨짐 방지)
        preview_text = hit.payload['text'][:200]
        if len(hit.payload['text']) > 200:
            preview_text += "..."
        print(f"   텍스트: {preview_text}")
        
        # 이미지 정보 출력
        if hit.payload.get('images'):
            print(f"   이미지 정보:")
            for i, img in enumerate(hit.payload['images'][:3]):  # 처음 3개만 출력
                caption_preview = img['caption'][:50] if img['caption'] else "캡션 없음"
                print(f"     {i+1}. {img['image_name']} - {caption_preview}...")
            if len(hit.payload['images']) > 3:
                print(f"     ... 외 {len(hit.payload['images']) - 3}개")
        print()
    
    return search_result

def test_search_functionality(qdrant_client, collection_name):
    """검색 기능 테스트"""
    
    print(f"\n🧪 검색 기능 테스트")
    print("=" * 50)
    
    # 다양한 한글 쿼리로 테스트
    test_queries = [
        "딸기 농장의 온도 관리",
        "환경 조절 시스템",
        "농작물 생육 관리",
        "비닐하우스 시설",
        "딸기 재배 기술",
        "온실 환경 관리",
        "농업 시설 자동화",
        "딸기 품질 관리"
    ]
    
    for query in test_queries:
        print(f"\n🔍 쿼리: '{query}'")
        search_similar_documents(qdrant_client, collection_name, query, limit=3)

def main():
    """메인 실행 함수"""
    
    # 환경변수 로드
    load_dotenv()
    
    # Voyage API 키 확인
    if not os.getenv("VOYAGE_API_KEY"):
        print("❌ VOYAGE_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    print("🚀 멀티모달 임베딩 및 벡터 저장 시작")
    print("=" * 50)
    
    # 1단계: step1에서 처리된 데이터 로드
    print("\n�� 1단계: 처리된 데이터 로드")
    processed_data = load_processed_data()
    
    if not processed_data:
        print("❌ 처리된 데이터를 찾을 수 없습니다. step1.py를 먼저 실행하세요.")
        return
    
    # 2단계: 멀티모달 시퀀스 생성
    print("\n🔄 2단계: 멀티모달 시퀀스 생성")
    sequences = create_multimodal_sequences(processed_data)
    
    if not sequences:
        print("❌ 시퀀스를 생성할 수 없습니다.")
        return
    
    # 3단계: Voyage AI 멀티모달 임베딩 생성
    print("\n�� 3단계: Voyage AI 임베딩 생성")
    voyage_client = voyageai.Client()
    embeddings = create_voyage_multimodal_embeddings(sequences, voyage_client)
    
    if not embeddings:
        print("❌ 임베딩을 생성할 수 없습니다.")
        return
    
    # 4단계: Qdrant에 저장
    print("\n�� 4단계: Qdrant에 저장")
    qdrant_client, collection_name = save_to_qdrant(embeddings)
    
    # 5단계: 검색 기능 테스트
    print("\n🔍 5단계: 검색 기능 테스트")
    test_search_functionality(qdrant_client, collection_name)
    
    print(f"\n✅ 단계 2 완료!")
    print(f"   다음 단계를 위해 다음 사항들을 확인하세요:")
    print(f"   - Qdrant 대시보드: http://localhost:6333/dashboard")
    print(f"   - 컬렉션: {collection_name}")
    print(f"   - 저장된 임베딩 수: {len(embeddings)}개")

if __name__ == "__main__":
    main()
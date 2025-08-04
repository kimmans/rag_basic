from dotenv import load_dotenv
load_dotenv()

import os
import json
import glob
from pathlib import Path
from PIL import Image
import voyageai
from qdrant_client import QdrantClient
from qdrant_client.http import models
import uuid
import re
import base64
from io import BytesIO

def load_parsed_data(parsed_dir="data/parsed"):
    """parsed 디렉토리에서 처리된 PDF 데이터를 로드"""
    
    parsed_files = []
    
    # 각 PDF별 디렉토리 순회
    for pdf_dir in Path(parsed_dir).glob("*"):
        if pdf_dir.is_dir():
            pdf_name = pdf_dir.name
            
            # 마크다운 파일 확인
            md_file = Path(parsed_dir) / f"{pdf_name}.md"
            captions_file = Path(parsed_dir) / f"{pdf_name}_captions.json"
            images_dir = pdf_dir / "images"
            
            if md_file.exists() and captions_file.exists() and images_dir.exists():
                # 마크다운 내용 로드
                with open(md_file, 'r', encoding='utf-8') as f:
                    md_content = f.read()
                
                # 캡션 정보 로드
                with open(captions_file, 'r', encoding='utf-8') as f:
                    captions = json.load(f)
                
                # 이미지 파일들 확인
                image_files = list(images_dir.glob("*.png"))
                
                parsed_files.append({
                    'pdf_name': pdf_name,
                    'md_content': md_content,
                    'captions': captions,
                    'image_files': image_files,
                    'images_dir': images_dir
                })
                
                print(f"✅ {pdf_name} 데이터 로드 완료")
                print(f"   - 마크다운: {len(md_content)} 문자")
                print(f"   - 캡션: {len(captions)}개")
                print(f"   - 이미지: {len(image_files)}개")
    
    return parsed_files

def create_multimodal_sequences(parsed_data):
    """마크다운 텍스트와 이미지를 인터리브하여 멀티모달 시퀀스 생성"""
    
    sequences = []
    
    for pdf_data in parsed_data:
        pdf_name = pdf_data['pdf_name']
        md_content = pdf_data['md_content']
        captions = pdf_data['captions']
        image_files = pdf_data['image_files']
        images_dir = pdf_data['images_dir']
        
        print(f"\n {pdf_name} 멀티모달 시퀀스 생성 중...")
        
        # 캡션 정보를 이미지 파일명으로 매핑
        caption_map = {caption['image']: caption for caption in captions}
        
        # 마크다운에서 이미지 참조 위치 찾기
        image_refs = list(re.finditer(r'<!-- image -->', md_content))
        
        if image_refs:
            # 이미지 참조가 있는 경우, 텍스트를 분할하여 인터리브 시퀀스 생성
            current_pos = 0
            multimodal_content = []
            
            for i, match in enumerate(image_refs):
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
                        
                        multimodal_content.append({
                            "type": "image", 
                            "content": f"data:image/png;base64,{img_data}",
                            "image_name": image_name,
                            "caption": caption_map.get(image_name, {}).get('caption', '')
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
    
    print(f"\n Voyage AI 멀티모달 임베딩 생성 중...")
    
    embeddings = []
    
    for i, sequence in enumerate(sequences):
        try:
            print(f"   [{i+1}/{len(sequences)}] 임베딩 생성: {sequence['pdf_name']}")
            
            # Voyage AI 멀티모달 API 형식으로 데이터 준비
            multimodal_input = []
            
            for item in sequence['multimodal_content']:
                if item['type'] == 'text':
                    multimodal_input.append({
                        "type": "text",
                        "text": item['content']
                    })
                elif item['type'] == 'image':
                    multimodal_input.append({
                        "type": "image",
                        "image": item['content']  # base64 encoded image
                    })
            
            # Voyage AI 멀티모달 임베딩 생성
            result = voyage_client.embed(
                multimodal_input,
                model="voyage-multimodal-3",
                input_type="document"
            )
            
            embedding = result.embeddings[0]
            
            embeddings.append({
                'pdf_name': sequence['pdf_name'],
                'multimodal_content': sequence['multimodal_content'],
                'embedding': embedding
            })
            
            print(f"   ✅ 멀티모달 임베딩 생성 완료")
            
        except Exception as e:
            print(f"   ❌ 임베딩 생성 실패: {e}")
            
            # 사용 가능한 메서드 확인
            available_methods = [method for method in dir(voyage_client) if not method.startswith('_')]
            print(f"   사용 가능한 메서드: {available_methods}")
    
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
    
    # 컬렉션 생성
    qdrant_client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(
            size=1024,  # Voyage 모델의 출력 벡터 차원
            distance=models.Distance.COSINE
        ),
        on_disk_payload=True
    )
    
    print(f"   컬렉션 생성: {collection_name}")
    
    # 포인트 생성
    points = []
    for i, emb_data in enumerate(embeddings):
        # 멀티모달 콘텐츠에서 텍스트 추출
        text_content = []
        for item in emb_data['multimodal_content']:
            if item['type'] == 'text':
                text_content.append(item['content'])
            elif item['type'] == 'image':
                text_content.append(item.get('caption', '[IMAGE]'))
        
        combined_text = " ".join(text_content)
        
        point = models.PointStruct(
            id=str(uuid.uuid4()),
            vector=emb_data['embedding'],
            payload={
                'pdf_name': emb_data['pdf_name'],
                'text': combined_text,
                'content_count': len(emb_data['multimodal_content'])
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
    
    # Voyage 클라이언트 생성
    voyage_client = voyageai.Client()
    
    try:
        if query_image:
            # 이미지와 텍스트로 검색
            multimodal_query = [
                {"type": "text", "text": query_text},
                {"type": "image", "image": query_image}
            ]
            result = voyage_client.embed(
                multimodal_query,
                model="voyage-multimodal-3",
                input_type="query"
            )
        else:
            # 텍스트만으로 검색
            result = voyage_client.embed(
                [{"type": "text", "text": query_text}],
                model="voyage-multimodal-3",
                input_type="query"
            )
        
        query_vector = result.embeddings[0]
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
        print(f"   텍스트: {hit.payload['text'][:200]}...")
        print()
    
    return search_result

def main():
    """메인 실행 함수"""
    
    # Voyage API 키 확인
    if not os.getenv("VOYAGE_API_KEY"):
        print("❌ VOYAGE_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    print(" Voyage AI 멀티모달 임베딩 시작")
    
    # 1. 파싱된 데이터 로드
    parsed_data = load_parsed_data()
    
    if not parsed_data:
        print("❌ 파싱된 데이터를 찾을 수 없습니다.")
        return
    
    # 2. 멀티모달 시퀀스 생성
    sequences = create_multimodal_sequences(parsed_data)
    
    if not sequences:
        print("❌ 시퀀스를 생성할 수 없습니다.")
        return
    
    # 3. Voyage AI 멀티모달 임베딩 생성
    voyage_client = voyageai.Client()
    embeddings = create_voyage_multimodal_embeddings(sequences, voyage_client)
    
    if not embeddings:
        print("❌ 임베딩을 생성할 수 없습니다.")
        return
    
    # 4. Qdrant에 저장
    qdrant_client, collection_name = save_to_qdrant(embeddings)
    
    # 5. 검색 테스트
    print(f"\n🧪 검색 테스트")
    search_similar_documents(
        qdrant_client, 
        collection_name, 
        "딸기 농장의 온도 관리와 환경 조절",
        limit=3
    )
    
    print(f"\n 모든 작업 완료!")

if __name__ == "__main__":
    main() 
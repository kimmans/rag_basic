import logging
import time
from pathlib import Path
from typing import List, TypedDict
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document
from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain_qdrant import QdrantVectorStore
from qdrant_client import QdrantClient, models
from qdrant_client.http.models import Distance, VectorParams
from langgraph.graph import StateGraph, END
from langchain_core.documents import Document as LangChainDocument
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
import os
from dotenv import load_dotenv

def setup_logging():
    """로깅 설정"""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)

def check_existing_qdrant_collections():
    """기존 Qdrant 컬렉션 확인"""
    
    print(f"\n🗄️ 기존 Qdrant 컬렉션 확인")
    print("=" * 50)
    
    client = QdrantClient(host="localhost", port=6333)
    
    try:
        collections = client.get_collections()
        print(f"   발견된 컬렉션: {len(collections.collections)}개")
        
        for collection in collections.collections:
            print(f"   - {collection.name}")
            
            # 컬렉션 정보 조회
            collection_info = client.get_collection(collection_name=collection.name)
            print(f"     벡터 수: {collection_info.points_count}")
            
            # 벡터 설정 정보
            if collection_info.config.params.vectors:
                print(f"     벡터 타입: {type(collection_info.config.params.vectors).__name__}")
        
        return collections.collections
        
    except Exception as e:
        print(f"   ❌ Qdrant 연결 실패: {e}")
        return []

def load_existing_qdrant_data(collection_name="voyage-multimodal-docs"):
    """기존 Qdrant 데이터 로드"""
    
    print(f"\n📊 기존 Qdrant 데이터 로드")
    print("=" * 50)
    
    client = QdrantClient(host="localhost", port=6333)
    
    try:
        # 컬렉션 정보 확인
        collection_info = client.get_collection(collection_name=collection_name)
        print(f"   컬렉션: {collection_name}")
        print(f"   저장된 벡터 수: {collection_info.points_count}")
        
        # 모든 포인트 조회
        points = client.scroll(
            collection_name=collection_name,
            limit=1000,  # 최대 1000개 포인트 조회
            with_payload=True,
            with_vectors=False
        )
        
        print(f"   로드된 포인트 수: {len(points[0])}")
        
        # 포인트 정보 출력
        for i, point in enumerate(points[0][:3]):  # 처음 3개만 출력
            print(f"   포인트 {i+1}:")
            print(f"     ID: {point.id}")
            print(f"     PDF: {point.payload.get('pdf_name', 'N/A')}")
            print(f"     콘텐츠 수: {point.payload.get('content_count', 'N/A')}")
            print(f"     이미지 수: {point.payload.get('image_count', 'N/A')}")
            print(f"     텍스트 미리보기: {point.payload.get('text', '')[:100]}...")
        
        if len(points[0]) > 3:
            print(f"   ... 외 {len(points[0]) - 3}개 포인트")
        
        return points[0]
        
    except Exception as e:
        print(f"   ❌ 데이터 로드 실패: {e}")
        return []

def create_retriever_from_existing_data(collection_name="voyage-multimodal-docs"):
    """기존 Qdrant 데이터를 사용한 리트리버 생성"""
    
    print(f"\n🔍 기존 데이터 기반 리트리버 생성")
    print("=" * 50)
    
    client = QdrantClient(host="localhost", port=6333)
    
    # 임베딩 모델 설정 (Dense 검색)
    dense_embeddings = OpenAIEmbeddings(
        model="text-embedding-3-small",
        api_key=os.getenv("OPENAI_API_KEY")
    )
    
    # 기존 컬렉션을 사용한 벡터 스토어 생성
    qdrant = QdrantVectorStore(
        client=client,
        collection_name=collection_name,
        embedding=dense_embeddings,
        vector_name="",  # 기존 컬렉션과 호환되도록 빈 문자열 사용
    )
    
    # 리트리버 생성
    retriever = qdrant.as_retriever(
        search_kwargs={"k": 10}
    )
    
    print(f"   ✅ 리트리버 생성 완료 (상위 10개 문서 검색)")
    
    return retriever

def print_search_results(docs, title):
    """검색 결과를 깔끔하게 출력하는 함수"""
    
    print(f"\n{title}")
    print("=" * 80)
    print(f"검색된 문서 수: {len(docs)}개\n")
    
    for i, doc in enumerate(docs, 1):
        print(f"📄 문서 {i}")
        print(f"📂 출처: {doc.metadata.get('pdf_name', 'N/A')}")
        print(f"📝 내용 미리보기:")
        print(doc.page_content[:300] + "..." if len(doc.page_content) > 300 else doc.page_content)
        print("-" * 40)

def setup_rag_workflow(retriever):
    """LangGraph를 사용한 RAG 워크플로우 설정"""
    
    print(f"\n🤖 RAG 워크플로우 설정")
    print("=" * 50)
    
    # State 정의
    class RAGState(TypedDict):
        question: str
        documents: List[LangChainDocument]
        answer: str
    
    # LLM 초기화 (GPT API 사용)
    llm = ChatOpenAI(
        model="gpt-4o",
        temperature=0,
        api_key=os.getenv("OPENAI_API_KEY")
    )
    
    # RAG 프롬프트 템플릿
    rag_prompt = ChatPromptTemplate.from_template("""
다음 문서들을 참고하여 질문에 답변해주세요.

문서들:
{context}

질문: {question}

답변은 한국어로 작성하고, 문서의 내용을 바탕으로 정확하고 자세하게 답변해주세요.
답변:
""")
    
    def retrieve_documents(state: RAGState) -> RAGState:
        """문서 검색 단계"""
        print("📚 문서 검색 중...")
        question = state["question"]
        documents = retriever.invoke(question)
        print(f"✅ {len(documents)}개의 문서를 검색했습니다.")
        
        return {
            "question": question,
            "documents": documents,
            "answer": ""
        }
    
    def generate_answer(state: RAGState) -> RAGState:
        """답변 생성 단계"""
        print("🤖 답변 생성 중...")
        question = state["question"]
        documents = state["documents"]
        
        # 문서 내용을 컨텍스트로 결합
        context = "\n\n".join([doc.page_content for doc in documents])
        
        # LLM 체인 구성
        chain = rag_prompt | llm | StrOutputParser()
        
        # 답변 생성
        answer = chain.invoke({
            "context": context,
            "question": question
        })
        
        print("✅ 답변이 생성되었습니다.")
        
        return {
            "question": question,
            "documents": documents,
            "answer": answer
        }
    
    # LangGraph 워크플로우 구성
    workflow = StateGraph(RAGState)
    
    # 노드 추가
    workflow.add_node("retrieve", retrieve_documents)
    workflow.add_node("generate", generate_answer)
    
    # 엣지 추가
    workflow.set_entry_point("retrieve")
    workflow.add_edge("retrieve", "generate")
    workflow.add_edge("generate", END)
    
    # 그래프 컴파일
    rag_app = workflow.compile()
    
    print("   ✅ RAG 워크플로우 설정 완료")
    
    return rag_app

def interactive_qa(rag_app):
    """대화형 Q&A 시스템"""
    
    print(f"\n💬 대화형 Q&A 시스템")
    print("=" * 50)
    print("질문을 입력하세요. 'quit', 'exit', 또는 '종료'를 입력하면 종료됩니다.")
    print("문서에 대한 질문을 자유롭게 해주세요!")
    print("-" * 50)
    
    while True:
        try:
            question = input("\n❓ 질문: ").strip()
            
            if question.lower() in ['quit', 'exit', '종료']:
                print("👋 대화형 Q&A 시스템을 종료합니다.")
                break
            
            if not question:
                continue
            
            print("🤖 답변 생성 중...")
            
            initial_state = {"question": question, "documents": [], "answer": ""}
            result = rag_app.invoke(initial_state)
            
            print(f"\n🤖 답변:")
            print(result["answer"])
            print(f"\n📚 참조된 문서 수: {len(result['documents'])}개")
            
        except KeyboardInterrupt:
            print("\n👋 대화형 Q&A 시스템을 종료합니다.")
            break
        except Exception as e:
            print(f"❌ 오류 발생: {e}")

def main():
    """메인 실행 함수"""
    
    # 환경변수 로드
    load_dotenv()
    
    print("🚀 RAG 시스템 시작")
    print("=" * 50)
    
    # 1단계: 기존 Qdrant 컬렉션 확인
    print("\n🗄️ 기존 Qdrant 컬렉션 확인 중...")
    collections = check_existing_qdrant_collections()
    
    if not collections:
        print("❌ Qdrant에 저장된 데이터가 없습니다. step1.py와 step2.py를 먼저 실행하세요.")
        return
    
    # 2단계: 기존 데이터 로드
    print("\n📊 기존 데이터 로드 중...")
    existing_data = load_existing_qdrant_data()
    
    if not existing_data:
        print("❌ 기존 데이터를 로드할 수 없습니다.")
        return
    
    # 3단계: 리트리버 생성
    print("\n🔍 리트리버 생성 중...")
    retriever = create_retriever_from_existing_data()
    
    # 4단계: RAG 워크플로우 설정
    print("\n🤖 RAG 워크플로우 설정 중...")
    rag_app = setup_rag_workflow(retriever)
    
    # 5단계: 대화형 Q&A 시스템 시작
    print("\n💬 대화형 Q&A 시스템을 시작합니다...")
    interactive_qa(rag_app)
    
    print(f"\n✅ RAG 시스템이 성공적으로 구축되었습니다!")
    print(f"   - Qdrant 대시보드: http://localhost:6333/dashboard")
    print(f"   - 기존 데이터 활용: {len(existing_data)}개 포인트")
    print(f"   - Dense 벡터 검색 지원 (OpenAI)")
    print(f"   - 대화형 Q&A 시스템 준비 완료")

if __name__ == "__main__":
    main() 
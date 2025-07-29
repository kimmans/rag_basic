# PDF Document Parser

PDF 문서를 파싱하여 마크다운 형식으로 변환하는 Python 프로젝트입니다.

## 기능

- PDF 파일을 LlamaParse를 사용하여 파싱
- VLM (Vision Language Model) 지원 (GPT-4o, Gemini Pro Vision)
- 마크다운 형식으로 결과 출력
- JSON 형식으로 구조화된 데이터 저장

## 설치

### 1. Poetry 설치
```bash
pip install poetry
```

### 2. 의존성 설치
```bash
poetry install
```

### 3. 가상환경 활성화
```bash
poetry shell
```

## 설정

### 1. 환경변수 설정
`.env` 파일을 생성하고 API 키를 설정하세요:

```env
# LlamaCloud API Key
LLAMA_CLOUD_API_KEY=your_llama_cloud_api_key_here

# OpenAI API Key (GPT-4o 사용시)
OPENAI_API_KEY=your_openai_api_key_here

# Google API Key (Gemini 사용시)
GOOGLE_API_KEY=your_google_api_key_here
```

### 2. PDF 파일 준비
`data/` 폴더에 파싱할 PDF 파일을 넣으세요.

## 사용법

### 기본 실행
```bash
python main.py
```

### 설정 변경
`main.py`에서 다음 설정을 변경할 수 있습니다:

```python
parser = LlamaParse(
    result_type="markdown",  # "markdown" 또는 "text"
    language="ko",           # 언어 설정
    parse_mode="parse_page_with_lvm",  # 파싱 모드
    vendor_multimodal_model_name="openai-gpt4o"  # 또는 "google-gemini-pro-vision"
)
```

## 출력 파일

- `parsed_result.json`: 구조화된 JSON 데이터
- `parsed_result.md`: 마크다운 형식의 텍스트

## 의존성

- `llama-cloud-services`: LlamaParse 서비스
- `python-dotenv`: 환경변수 관리
- `nest-asyncio`: 비동기 처리

## 라이선스

MIT License 
# Chapter 4 : 구조적 API 개요

[image reference] ([https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/ch04.html](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/ch04.html))

### 스파크 기본 개념 Review

- transformation의 처리 과정을 정의하는 분산 프로그래밍 모델
- transformation은 DAG(비순환 그래프)로 표현되는 명령을 만들어 낸다.
- action은 하나의 잡을 클러스터에서 실행하기 위해 stage/task로 나누고 프로세스를 실행한다.
- transformation과 action을 다루는 논리적 구조 → DataFrame & Dataset

### 구조적 API

- 배치와 스트리밍 처리에서 사용
- 배치, 스트리밍 작업 상호 변환 용이
- 데이터 흐름을 정의하는 기본 추상화 개념

### 구조적 API의 세 가지 분산 컬렉션 API

- Dataset
- DataFrame
- SQL 테이블과 뷰

### 스키마

- DataFrame의 컬럼명과 데이터 타입을 정의
- 데이터 소스에서 얻거나 (schema-on-read) 직접 정의

### Catalyst Engine

- catalyst engine을 통해 다른 언어로 작성된 표현식을 스파크 데이터 타입으로 변환하여 처리
- 실행 계획 수립과 처리에 사용하는 자체 타입 정보를 가지고 있어 다양한 실행 최적화 기능을 제공

스파크 2.0 이후로 DataFrame과 Dataset은 통합되었다.

### DataFrame

### Dataset

- 잘 정리된 row와 column을 가지는 분산 테이블 형태의 컬렉션
- 결과 생성을 위해 어떤 데이터에 어떤 연산을 적용할지 정의하는 지연 연산의 실행 계획. 불변성

---

- 비타입형
- 스키마와 타입 일치 런타임에 확인

- 타입형
- 스키마와 타입 일치 컴파일 타임에 확인
- 컴파일 타입에 엄격하다면

### 구조적 API의 실행 과정

1. DataFrame/Dataset/SQL을 이용해 코드 작성
2. 정상적인 코드라면 논리적 실행 계획으로 변환
3. 논리적 실행 계획 → 물리적 실행 계획 변환 (추가적인 최적화 가능 여부 확인)
4. 클러스터에서 물리적 실행 계획 (RDD 처리) 실행

1. 코드 작성

![Chapter%204%20%E1%84%80%E1%85%AE%E1%84%8C%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%A8%20API%20%E1%84%80%E1%85%A2%E1%84%8B%E1%85%AD%2064c48b55c48244a9976b8a9eb126939e/Untitled.png](Chapter%204%20%E1%84%80%E1%85%AE%E1%84%8C%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%A8%20API%20%E1%84%80%E1%85%A2%E1%84%8B%E1%85%AD%2064c48b55c48244a9976b8a9eb126939e/Untitled.png)

- 작성한 코드를 콘솔이나 spark-submit으로 실행
- catalyst optimizer가 실제 실행 계획 생성
- 스파크가 코드 실행 후 결과 반환

2. 논리적 실행 계획

![Chapter%204%20%E1%84%80%E1%85%AE%E1%84%8C%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%A8%20API%20%E1%84%80%E1%85%A2%E1%84%8B%E1%85%AD%2064c48b55c48244a9976b8a9eb126939e/Untitled%201.png](Chapter%204%20%E1%84%80%E1%85%AE%E1%84%8C%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%A8%20API%20%E1%84%80%E1%85%A2%E1%84%8B%E1%85%AD%2064c48b55c48244a9976b8a9eb126939e/Untitled%201.png)

- 사용자 코드를 논리적 실행 계획으로 변환 (추상적 트랜스포메이션)
- driver executor의 정보를 고려하지 않음
- 표현식을 최적화된 버전으로 변환
- 코드 → 검증 전 논리적 실행 계획 (코드의 유효성과 테이블/컬럼의 존재 여부만 판단)
- 스파크 분석기 - 컬럼과 테이블 검증
- 검증 결과를 catalyst optimizer로 전달

3. 물리적 실행 계획

![Chapter%204%20%E1%84%80%E1%85%AE%E1%84%8C%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%A8%20API%20%E1%84%80%E1%85%A2%E1%84%8B%E1%85%AD%2064c48b55c48244a9976b8a9eb126939e/Untitled%202.png](Chapter%204%20%E1%84%80%E1%85%AE%E1%84%8C%E1%85%A9%E1%84%8C%E1%85%A5%E1%86%A8%20API%20%E1%84%80%E1%85%A2%E1%84%8B%E1%85%AD%2064c48b55c48244a9976b8a9eb126939e/Untitled%202.png)

- 논리적 실행 계획을 클러스터 환경에서 실행하는 방법 정의
- 최적의 전략 선택 (비용 비교 예시: 조인 연산 수행 비용 비교)
- 물리적 실행 계획 → RDD, Transformation으로 컴파일

4. 실행

- RDD를 대상으로 모든 코드 실행
- 추가적인 최적화, 처리 결과를 사용자에 반환
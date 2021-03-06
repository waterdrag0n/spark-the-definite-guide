# Chapter 2 : 스파크 간단히 살펴보기

- 스파크: 컴퓨터 클러스터의 데이터 처리 작업 관리 및 조율
- 사용자(클러스터 매니저에 스파크 애플리케이션 제출) - 클러스터 매니저(자원 할당)

### 스파크 애플리케이션

- Driver Process
    - 클러스터 노드 중 하나에서 실행되며 main() 함수 실행.
    - 정보 유지 관리, 프로그램이나 입력 응답, 프로세스 작업 관련 분석, 배포, 스케줄링
    - 스파크 애플리케이션의 심장과 같은 존재, 수명 주기 동안 관련 정보 모두 저장
- Executor Process
    - 드라이버 프로세스가 할당한 작업 수행.
    - 할당한 코드를 실행하고 보고하는 두 가지 역할.

![2-1](https://user-images.githubusercontent.com/70019911/127878905-8fcabd74-8d4c-4c27-810a-7afbfe8dc7bc.png)

### 스파크 API

- 저수준 비구조적(unstructured) API
- 고수준 구조적(structured) API

### SparkSession

- 스파크 애플리케이션은 SparkSession(드라이버 프로세스)으로 제어
- 사용자가 정의한 처리 명령 클러스터에서 실행
- 1개 SparkSession - 1개 스파크 애플리케이션 대응

### DataFrame

- 테이블의 데이터를 row와 column으로 단순하게 표현
- 스키마 - 컬럼, 컬럼의 타입을 정의한 목록
- 크기가 너무 크거나 계산에 시간이 너무 오래 걸려 여러 컴퓨터에 분산되어 있음

### Partition

- 클러스터의 물리적 머신에 존재하는 로우의 집합
- 파티션(청크) 단위로 데이터 분할
- 파티션 == 병렬성
- DataFrame 사용하면 파티션 개별적으로 처리하지 않아도 됨 (스파크가 결정)

### Transformation

- 스파크 핵심 데이터 구조는 불변성을 가짐
- DataFrame을 변경하려면 변경 방법을 스파크에 알려줘야 함(명령: Transformation)
- 두 가지 유형
1. 좁은 의존성 (narrow dependency)
    - 각 입력 파티션이 하나의 출력 파티션에만 영향을 미침 (1:1)
2. 넓은 의존성 (wide dependency)
    - 하나의 입력 파티션이 여러 출력 파이션에 영향을 미침 (1:N)

### 지연 연산

- 스파크는 연산 명령이 내려지면 바로 수행하지 않고 트랜스포메이션의 실행 계획을 생성함
- 마지막 순간까지 대기하다가 한번에 컴파일
- 전체 데이터 흐름을 최적화하는 효과

### Action

- 실제 연산을 수행하기 위한 명령
- 액션을 시작하면 Spark job 시작
- 좁은 트랜스포메이션 - 넓은 트랜스포메이션

### Spark UI

- 스파크 잡의 진행 상황을 모니터링 할 때 사용
- 스파크 잡의 상태, 환경 설정, 클러스터 상태 등
- Driver Node - 4040 port

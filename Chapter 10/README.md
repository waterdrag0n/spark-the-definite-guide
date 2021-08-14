# Chapter 10: 스파크 SQL

### 10.1 SQL이란

- 데이터에 대한 관계형 연산을 표현하기 위한 도메인 특화 언어

### 10.2 빅데이터와 SQL: 아파치 하이브

- 스파크 이전에는 하이브가 빅데이터 SQL에서 사실상 표준.
- 스파크는 RDD 이용 범용 엔진으로 출발하여 스파크 SQL 사용자를 늘림

### 10.3 빅데이터와 SQL: 스파크 SQL

- 스파크 SQL의 강력함
    - SQL 분석가: 쓰리프트 서버나 SQL 인터페이스에서 활용
    - 데이터 엔지니어, 과학자: 전체 데이터 처리 파이프라인에서 활용
    - 통합형 API: SQL로 데이터 조회하고 df로 변환한 다음 스파크 MLlib에서 제공하는 머신러닝 알고리즘을 이용해 수행한 결과를 다른 데이터소스에 저장하는 과정 가능
- 10.3.1 스파크와 하이브의 관계
    - 스파크 SQL이 하이브 메타스토어를 사용하여 하이브와 잘 연동됨
        - 하이브 메타스토어: 여러 세션에서 사용할 테이블 정보 보관
    - 스파크 SQL은 하이브 메타스토어에 접속하여 조회할 파일 수를 최소화하기 위해 메타스토어 참조

### 10.4 스파크 SQL 쿼리 실행 방법

- 10.4.1 스파크 SQL CLI
    - 로컬 환경의 명령행에서 기본 스파크 SQL 쿼리를 실행할 수 있는 도구
    - thrift JDBC 서버와는 통신 불가
    - 실행 명령: ./bin/spark-sql
- 10.4.2 스파크의 프로그래밍 SQL 인터페이스
    - 서버를 사용하지 않고 스파크에서 지원하는 언어 API로 비정형 SQL 실행 가능
    - SparkSession 객체의 sql 메서드 사용 - df 반환
- 10.4.3 스파크 SQL 쓰리프트 JDBC/ODBC 서버
    - 스파크는 자바 데이터베이스 연결 인터페이스 제공
    - 사용자나 원격 프로그램은 스파크 SQL을 실행하기 위해 이 인터페이스로 스파크 드라이버에 접속

### 10.5 카탈로그

- 스파크 SQL에서 가장 높은 추상화 단계
- 테이블에 저장된 데이터에 대한 메타데이터뿐 아니라 db, table, 함수, 뷰에 대한 정보를 추상화

### 10.6 테이블

- 스파크 SQL로 작업을 수행하려면 먼저 테이블 정의
- df와 논리적으로 동일하나, df는 프로그래밍 언어로 정의하고 테이블은 db에서 정의
- 테이블을 생성하면 default db에 등록
- 임시 테이블의 개념이 없이 테이블은 항상 데이터를 가지고 있음
    - 테이블을 제거하면 모든 데이터가 제거됨
- 10.6.1 스파크 관리형 테이블
    - 테이블이 저장하는 두 가지 중요한 정보
        - 테이블의 데이터
        - 테이블에 대한 데이터 (메타데이터)
    - 디스크에 저장된 파일을 이용해 테이블을 정의하면 외부 테이블을 정의하는 것
    - savedAsTable
        - DataFrame의 savedAsTable 메서드는 스파크가 관련된 모든 정보를 추적할 수 있는 관리형 테이블을 만들 수 있음
        - 테이블을 읽고 데이터를 스파크 포맷으로 변환 후 새로운 경로에 저장
        - 저장 경로 하위에서 db 목록 확인 가능

```python
# 10.6.2 테이블 생성하기
# 원래 SQL처럼 테이블을 정의한 다음 테이블에 데이터를 적재하지 않고 실행 즉시 테이블 생성
CREATE TABLE flights (DEST_COUNTRY_NAME STRING COMMENT 'comment', ...)
USING JSON OPTIONS (path 'path')

# USING: 포맷 지정
# 코멘트 추가 가능

# SELECT의 결과를 이용해 테이블 생성 또한 가능
REATE TABLE flights_from_select USING parquet AS SELECT * FROMM flights

# 테이블이 없는 경우에만 생성
IF NOT EXISTS

# 파티셔닝된 데이터셋 저장
PARTITIONED BY (DEST_COUNTRY_NAME)

# 10.6.3 외부 테이블 생성하기
# 기존 하이브 쿼리문을 스파크 SQL로 변환해야 하는 상황
# 대부분은 바로 사용할 수 있음
# 외부 테이블 생성
# 스파크는 외부 테이블의 메타데이터를 관리하지만 데이터 파일은 관리하지 않음

CREATE EXTERNAL TABLE hive_flights (column type, ....)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'path'

# or

CREATE EXTERNAL TABLE hive_flights_2
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'path' AS SELECT * FROM flights

# 10.6.4 테이블에 데이터 삽입하기
# 표준 SQL 문법을 따름
INSERT INTO flights_from_select SELECT column1, column2 FROM flights LIMIT 20

# 특정 파티션에만 저장하고 싶은 경우 (속도가 매우 느려질 수 있음)
PARTITION (DEST_COUNTRY_NAME='UNITED STATES')

# 10.6.5 테이블 메타데이터 확인하기
# 테이블 생성 시 추가한 코멘트 확인
DESCRIBE TABLE flights_csv
# 파티셔닝 스키마 정보 확인 (파티션된 테이블에서만)
SHOW PARTITIONS partitioned_flights

# 10.6.6 테이블 메타데이터 갱신하기
# 테이블 메타데이터를 유지하는 것 - 가장 최신의 데이터셋을 읽고 있는 것을 보장
# 메타데이터를 갱신할 수 있는 두 가지 방법
# 1
REFRESH table partitioned_flights
# 2
MSCK REPAIR TABLE partitioned_flights

# 10.6.7 테이블 제거하기
# 테이블은 삭제(delete)할 수 없고 제거(drop)만 가능
DROP TABLE flights_csv; # 테이블, 데이터 모두 제거

# 없는 테이블을 제거하면 오류 발생. 존재하는 경우에만 제거하려면
DROP TABLE IF EXISTS flights_csv;

#외부 테이블을 제거하면 데이터는 삭제되지 않고 참조할 수 없게 됨.

# 10.6.8 테이블 캐싱하기
CACHE TABLE flights
UNCACHE TABLE flights
```

DROP, DELETE, TRUNCATE의 차이점

DROP: 테이블 자체를 삭제

DELETE: 데이터를 하나하나 지움. (<> TRUNCATE). 
데이터는 사라지고 테이블은 사라지지 않음.

TRUNCATE: 테이블의 데이터가 모두 사라지지만 테이블은 사라지지 않음. 
CREATE TABLE을 한 직후의 상태. 데이터를 한 번에 지움.

### 10.7 뷰

- 뷰는 기존 테이블에 여러 트랜스포메이션 작업 지정
- 단순 쿼리 실행 계획
- 뷰를 사용해서 쿼리 로직을 체계화, 재사용하기 편하게 만들 수 있음

```python
# 10.7.1 뷰 생성하기
# 최종 사용자에게 뷰는 테이블처럼 보임
# 신규 경로에 모든 데이터를 다시 저장하지 않고
# 단순히 쿼리 시점에 데이터소스에 트랜스포메이션 실행
CREATE VIEW just_usa_view AS
SELECT * FROM flights WHERE dest_country_name = 'United States'

# db에 등록되지 않고 현재 세션에서만 사용하는 임시 뷰 생성
CREATE TEMP VIEW

#global temp view : db에 상관 없이 사용할 수 있지만 세션이 종료되면 사라짐
CREATE GLOBAL TEMP VIEW

# 생성된 뷰 덮어쓰기
CREATE OR REPLACE TEMP VIEW

#실행 계획
EXPLAIN SELECT * FROM just_usa_view

# 10.7.2 뷰 제거하기
# 테이블 제거와 동일하지만 대상을 뷰로 지정하면 됨
# 뷰 제거는 어떤 데이터도 제거되지 않고 뷰 정의만 제거됨
DROP VIEW IF EXISTS just_usa_view
```

### 10.8 데이터베이스

- 여러 테이블을 조직화하기 위한 도구
- 정의하지 않으면 기본 db 사용

```python
# 전체 db 목록 확인
SHOW DATABASES

# 10.8.1 데이터베이스 생성하기
CREATE DATABASE some_db

# 10.8.2 데이터베이스 설정하기
USE some_db

# 모든 쿼리는 앞서 지정한 db를 참조하지만 실패하거나 다른 결과를 반환할수도 있음
# 접두사를 이용
SELECT * FROM default.flights

# 현재 사용중인 db 확인
SELECT current_database()

# 10.8.3 데이터베이스 제거하기
DROP DATABASE IF EXISTS some_db
```

### 10.9 select 구문

```python
# SQL 쿼리의 값을 조건에 맞게 변경할 경우
# case...when...then...end를 이용해 처리 (if문과 동일)
SELECT
	CASE WHEN DEST_COUNTRY_NAME = 'UNITED STATES' THEN 1
			 WHEN DEST_COUNTRY_NAME = 'EGYPT' THEN 0
			 ELSE -1 END
FROM partitioned_flights
```

### 10.10 고급 주제

```python
# 10.10.1 복합 데이터 타입
# 표준 SQL에 존재하지 않는 강력한 기능
# 3가지 핵심 복합 데이터 타입 - 구조체, 리스트, 맵

# 구조체
# 맵에 가까움. 중첩 데이터를 생성하거나 쿼리하는 방법 제공
# 여러 컬럼이나 표현식을 괄호로 묶으면 됨
CREATE VIEW IF NOT EXISTS nested_data AS
	SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) as country, count FROM flights

# 리스트
# 다른 언어에서의 리스트(배열)과 같음
# 값의 리스트 만들기 - collect_list
# 중복 값 없는 리스트 - collect_set
SELECT DEST_COUNTRY_NAME as new_name, collect_list(count) as flight_counts,
collect_set(ORIGIN_COUNTRY_NAME) as origin_set
FROM flights GROUP BY DEST_COUNTRY_NAME

#배열 직접 생성
SELECT DEST_COUNTRY_NAME, ARRAY(1,2,3) FROM flights

#특정 위치 데이터 쿼리
collect_list(count)[0]

# 다시 여러 로우로 변환
# collect와 완전히 반대이기 때문에 collect 실행 전과 같은 결과
SELECT explode(collected_counts) FROM flights_agg

# 10.10.2 함수
# 전체 함수 목록 확인
SHOW FUNCTIONS

# 시스템 함수, 사용자 함수 확인
SHOW SYSTEM FUNCTIONS
SHOW USER FUNCTIONS

# 와일드카드 문자로 결과 필터링
# 예시) S로 시작하는 모든 함수
SHOW FUNCTIONS 'S*'

#LIKE 키워드
SHOW FUNCTIONS LIKE 'collect*'

#사용자 정의 함수
def power3(...):
	...

spark.udf.register('power3'. power3(...))

SELECT count, power3(count) FROM flights

# 10.10.3 서브쿼리
# 서브쿼리를 사용하여 쿼리 안에 쿼리 지정
# 정교한 로직 명시

# 상호연관 서브쿼리
# 서브쿼리의 정보를 보완하기 위해 커리 외부 범위에 있는 일부 정보 사용

# 비상호연관 서브쿼리
# 외부 정보 사용 x

# 조건절 서브쿼리
# 값에 따라 필터링

# 비상호연관 조건절 서브쿼리
SELECT * FROM fligths
WHERE origin_country_name IN (SELECT dest_country_name FROM flights 
GROUP BY dest_country_name ORDER BY sum(count) DESC LIMIT 5)

# 상호연관 조건절 서브쿼리
SELECT * FROM flights f1
WHERE EXISTS (SELECT 1 FROM flights f2
							WHERE f1.dest_country_name = f2.origin_country_name)
AND EXISTS (SELECT 1 FROM flights f2
							WHERE f2.dest_country_name = f1.origin_country_name)

# 비상호연관 스칼라 쿼리
# 기존에 없던 일부 부가 정보 가져올 수 있음
SELECT *, (SELECT max(count) FROM flights) AS maximum FROM flights
```

### 10.11 다양한 기능

- SQL에서 설정값 지정하기

```python
SET spark.sql.shuffle.partitions=20
```
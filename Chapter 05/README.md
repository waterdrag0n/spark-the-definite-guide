# Chapter 5 : 구조적 API 기본 연산

## DataFrame

- record(row) + column
- schema: 각 컬럼명과 데이터 타입 정의
- partitioning: 클러스터에서 물리적으로 배치되는 형태 정의
- partitioning schema: partition을 배치하는 방법을 정의
    - partitioning의 분할 기준은 특정 컬럼이나 비결정론적(매번 변하는) 값을 기반으로 설정

### Schema

- 대부분 schema-on-read가 잘 동작하지만 정밀도 문제가 생기면 직접 정의해야 함
- 여러 개의 StructField로 구성된 StructType
    - StructField - ${이름}, ${데이터타입}, ${컬럼이 값이 없거나 null일 수 있는가?}, ${컬럼 관련 메타데이터} (필요한 경우)
    - StructType - 복합 데이터 타입
- 런타임에 데이터 타입이 schema 데이터 타입과 불일치하면 에러

df.createOrReplaceTempView("<view_name>")
DataFrame에 SQL을 사용하기 위해

## Column

- 표현식을 사용해 레코드 단위로 계산한 값을 단순하게 나타내는 논리적 구조
- DataFrame을 통하지 않으면 외부에서 column에 접근 불가
- col(${컬럼명})으로 참조
- 컬럼이 실제로 있을지 알 수 없음 - 분석기가 동작하는 단계에서 확인 가능
- 명시적 컬럼 참조
    - col()을 사용해서 명시적으로 컬럼을 참조하면 스파크 분석기 실행 단계에서 컬럼 확인 절차를 생략함

### 표현식

- 레코드의 여러 값에 대한 transformation 집합
- 여러 컬럼명을 입력으로 단일 값을 만들기 위해 다양한 표현식을 각 레코드에 적용하는 함수
- 표현식을 통해 컬럼을 select, control, remove
- expr() - DataFrame 컬럼 참조
- 스파크가 연산 순서를 논리적 트리로 컴파일 하기 때문에 아래 두 예시가 모두 같은 트랜스포메이션 과정을 거치게 됨

    ```python
    (((col("someCol") + 5) * 200) - 6) < col("otherCol")

    import pyspark.sql.functions import expr
    expr("(((someCol + 5) * 200) - 6) < otherCol")
    ```

![Untitled](https://user-images.githubusercontent.com/70019911/127879106-b9ede38a-c541-4aaa-a545-aba852ba600d.png)

- SQL과 DataFrame 코드 모두 동일한 논리 트리로 컴파일 되며 성능 또한 동일함
- printSchema, columns - 전체 컬럼 정보 확인

## Record와 Row

- 각 로우 == 하나의 레코드
- Row 객체는 내부에 바이트 배열을 가짐
    - 컬럼 표현식으로만 제어 가능
    - 사용자에 노출되지 않음
- Row() - Row 객체 생성
    - 스키마와 같은 순서로 값을 명시해야 함
- [idx]로 접근

## DataFrame의 Transformation

- 주요 작업
    1. row, column 추가
    2. row, column 제거
    3. row ↔ column 변환
    4. column값 기준으로 row 순서 변경

### select / selectExpr

- DataFrame에서도 SQL 사용 가능하게 해줌

```python
#python
df.select("COLUMN_NAME1", "COLUMN_NAME2").show(2)
#SQL
SELECT COLUMN_NAME1, COLUMN_NAME2 FROM dfTable LIMIT 2
```

- alias("newName") - newName으로 컬럼명 변경 (SQL AS)
- select 메서드에 expr 함수를 자주 사용하는데, 이를 간단하게 할 수 있는 selectExpr 제공

```python
#python
df.selectExpr(
	"*", #모든 원본 컬럼 포함
	"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
	.show(2)

#SQL
SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry FROM dfTable LIMIT 2
```

- avg, count와 같은 집계함수도 표현식에서 지정 가능

### 스파크 데이터 타입으로 변환하기

- 명시적인 값을 전달해야 할 때 literal 사용
- 어떤 상수나 변숫값이 특정 컬럼의 값보다 큰지 확인할 때 사용

### 컬럼 추가하기

- withColumn(${컬럼이름}, ${값을 생성할 표현식})
- 이를 이용해서 컬럼명을 변경할 수도 있음
- withColumnRenamed(${기존 이름}, ${바꿀 이름})
- 컬럼명 예약 문자, 키워드
    - "  "(공백), "-"
    - ``을 통해 예약 문자 사용 가능(escaping)
    - e.g "`column-name`"
- set spark.sql.caseSensitive true - 대소문자 구분하도록

### 컬럼 제거하기

```python
#컬럼 제거하기
df.drop("COLUMN_NAME1", "COLUMN_NAME2")

#컬럼 데이터 타입 변경하기
#컬럼명 count → count2, 타입 int → string
df.withColumn("count2", col("count").cast("string"))
SELECT *, cast(count as string) AS count2 FROM dfTable
```

### Row Filtering

- where, filter

```python
df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)
SELECT * FROM dfTable WHERE count < 2 LIMIT 2

#여러 필터 적용 시
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia").show(2)
SELECT * FROM dfTable WHERE count < 2 AND ORIGIN_COUNTRY_NAME != "Croatia" LIMIT 2
```

### 고유한 row 얻기

- distinct()

```python
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable
```

### Random Sample

```python
df.sample(withReplacement, fraction, seed).count()
```

### 임의 분할

- ML에서 train set, test set 분할할 때 주로 사용

```python
df.randomSplit([0.25, 0.75], seed)
```

### Row 합치기 & 추가하기

- DataFrame은 불변성을 가지기 때문에 레코드를 추가하려면 원본을 새로운 DataFrame과 통합
    - 동일한 스키마, 컬럼 수를 가져야 함

```python
df.union(newDF)\
	.where("count=1")\
	.where(col("ORIGIN_COUNTRY_NAME") != "United States")\
	.show()
```

### Row 정렬하기

- sort, orderBy

```python
df.sort("count").show(5)
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

#파티션별 정렬 - 성능 최적화
df.sortWithinPartitions("count")
```

### Row 수 제한하기

```python
df.limit(5).show()
```

### repartition & coalesce

- 다른 최적화 기법 - 자주 필터링하는 컬럼을 기준으로 데이터를 분할
- repartition - 전체 데이터 셔플
    - 향후 사용할 파티션 수가 현재보다 많거나 컬럼을 기준으로 파티션을 만드는 경우에만 사용
- coalesce - 전체 데이터를 셔플하지 않고 파티션을 병합하는 경우

### 드라이버로 로우 데이터 수집하기

- 스파크는 드라이버에서 클러스터 상태 정보 유지
- 드라이버로 데이터를 수집하는 연산
    - collect - 모든 데이터 수집
        - 큰 비용이 발생해 드라이버가 비정상적으로 종료될 수 있음
    - take - 상위 n개 로우 반환
    - show - 여러 로우를 보기 좋게 출력
- toLocalIterator
    - 전체 데이터셋에 대한 반복 처리를 위해 드라이버로 로우를 모으는 방법
    - iterator로 모든 파티션 데이터를 드라이버에 전달
    - 큰 비용이 발생해 드라이버가 비정상적으로 종료될 수 있음

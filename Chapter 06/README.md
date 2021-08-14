# Chapter 6: 다양한 데이터 타입 다루기

### 6.1 API는 어디서 찾을까

- 데이터 변환용 함수가 책의 내용에서 변경될 경우 찾는 방법
    - DataFrame 메서드 - DataFrameStatFunctions, DataFrameNaFunctions
    - Column 메서드 - 공식 문서

### 6.2 스파크 데이터 타입으로 변경하기

- 데이터 타입 변환 - lit

```python
from pyspark.sql.functions import lit

df.select(lit(5), lit("five"), lit(5.0))
```

### 6.3 불리언 데이터 타입 다루기

- and, or, true, false

```python
from pyspark.sql.functions import col

df.where(col("InvoidNo") != 536365) \
	.select("InvoiceNo", "Description") \
	.show(5, False)

df.where("InvoiceNo = 536365")
df.where("InvoiceN0 <> 536365") #!=

# and나 or을 사용하면 표현식을 여러 부분에 지정 가능
# 모든 표현식을 and로 묶어 차례대로 필터 적용해야 함
from pyspark.sql.functions import instr

priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()

#SQL
SELECT * FROM dfTable WHERE StackCode in ("DOT") 
	AND(UnitPrice > 600 OR instr(Description, "POSTAGE") >=1)))

# 불리언 컬럼으로 DataFrame 필터링 가능
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter)\
	where("isExpensive").select("unitPrice", "isExpensive").show()

# null-safe
df.where(col("Description").eqNullSafe("safe")).show()
```

### 6.4 수치형 데이터 타입 다루기

```python
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
df.select(expr("CustomerId"), fabricatedQuantity.alias("realQuantity")).show()

SELECT customerId, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity

# 반올림, 내림
round, bround

#피어슨 상관계수
corr

#요약 통계 - describe
df.describe().show()

#정확한 수치
from pyspark.sql.functions import count, mean, stddev_pop, min, max

#백분위수, 근사치
df.stat.approxQuantile(...)

#교차표
df.stat.crosstab(...).show()

#모든 로우에 고유 ID값 추가
monotonically_increasing_id

#랜덤값
rand(), randn()
```

### 6.5 문자열 데이터 타입 다루기

```python
# 대/소문자 변환
# 주어진 문자열에서 공백으로 나뉘는 모든 단어의 첫 글자를 대문자로 변경
initcap # hello world -> Hello World

lower, upper

#문자열 주변의 공백을 제거, 추가
ltrim # ___hello___ -> hello___
rtrim # ___hello___ -> ___hello
trim # ___hello___ -> hello
lpad(lit("hello"), 3, " ") #hello -> _he
rpad(lit("hello"), 10, " ") #hello -> hello_____
```

- 6.5.1 정규표현식

```python
regexp_extract , regexp_replace

#regexp_replace를 이용해 description 컬럼의 값을 COLOR로 치환
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(regexp_replace(col("Description"), regex_string, "COLOR")
	.alias("color_clean"), col("Description")).show()

# translate을 이용해 문자 치환. 해당 문자 모두 치환
df.select(translate(col("Description"), "LEFT", "1337"), col("Description")).show()

#해당하는 첫번째 추출
regexp_extract

#값 추출 없이 존재 여부만 확인
instr

#문자열의 위치를 정수로 반환. 쉽게 확장
locate
```

### 6.6 날짜와 타임스탬프 데이터 타입 다루기

- 스파크는 두 종류의 시간 관련 정보만 관리
    - date / timestamp
    - TimestampType은 초 단위 정밀도만 지원
    - long 데이터 타입으로 변환하면 정밀한 단위 사용 가능

```python
date_sub(col("today"), 5)
date_add. ...

#날짜 차이
datediff #일
months_between #월

#문자열을 날짜로 변환, 포맷 지정도 가능
to_date #날짜 파싱 안되면 null 반환

#항상 날짜 포맷 지정해야 함
to_timestamp

#날짜 비교
df.filter(col("date") > lit("2021-08-03")).show() 
```

### 6.7 null 값 다루기

- 빠져 있거나 비어 있는 데이터를 null로 표현하면 좋음
    - 최적화 수행하기 용이
    - DataFrame의 하위 패키지 .na 사용 or 제어 방법 명시적으로 지정

```python
# 6.7.1 coalesce
# 인수로 지정한 여러 컬럼 중 null이 아닌 첫 값 반환
# 모든 컬럼이 null이 아니면 첫 컬럼의 값 반환

df.select(coalesce(col("Descriptoin"), col("customerId"))).show()

# 6.7.2 ifnull, nullIf, nvl, nvl2

# ifnull - 첫 번째 값이 null이면 두 번째 값 반환
# nullif - 두 값이 같으면 null 반환, 다르면 첫 번째 값 반환
# nvl - 첫 번째 값이 null이면 두 번째 값 반환, null이 아니면 첫 번째 값 반환
# nvl2 - 첫 번째 값이 null이 아니면 두 번째 값 반환, null이면 세 번째 인수로 지정된 값 반환

SELECT
	ifnull(null, 'return_value'), # return_value
	nullif('value', 'value'), # null
	nvl(null, 'return_value'), #return_value
	nvl2('not_null', 'return_value' "else_value") # return_value
FROM dfTable LIMIT 1

# 6.7.3 drop
df.na.drop() #null값 가진 로우 모두 제거
SELECT * FROM dfTable WHERE description IS NOT NULL

df.na.drop("all") # 모든 값이 null이면 drop

# 6.7.4 fill
# 하나 이상의 컬럼을 특정 값으로 채움
df.na.fill("string") # null값을 string으로 채워줌
df.na.fill(5:Integer) # 정수형 null 값에 다른 값 채워줌

# map
fill_cols_vals = {"StockCode":5, "Description": "No Value"}
df.na.fill(fill_cols_vals)

#6.7.5 replace
# 조건에 따라 대체. 원래와 변경 값의 타입이 같아야 함
df.na.replace([""], ["UNKNOWN"], "Description")
```

### 6.8 정렬하기

- acs_nulls_first
- desc_nulls_first
- asc_nulls_last
- sesc_nulls_last

### 6.9 복합 데이터 타입 다루기

- 구조체, 배열, 맵

```python
# 6.9.1 구조체
# DataFrame 내부의 DataFrame
# 다수의 컬럼을 괄호로 묶어 구조체 생성

complexDF = df.select(struct("Description", "InvoiceNo").alias("complex")

# 6.9.2 배열
# split
df.select(split(col("Description"), " ")).show() # space로 split
size(split(col("Description"), " ")) # size
array_contains(split(col("Description"), " "), "WHITE") #WHITE가 포함되어 있는지

#explode
#배열 타임의 컬럼을 입력받고 모든 값을 로우로 변환
# "Hello World", "Other Col" -split-> ["Hello", "World"], "Other Col"
#  -explode-> "Hello", "Other Col", "World", "Other Col"

#6.9.3 맵
# key-value
create_map(col("Description"), col("InvoiceNo"))
#조회
selectExpr("map_name['key']").show()
```

### 6.10 JSON 다루기

- JSON을 파싱하거나 객체로 만들 수 있음

```python
jsonDF = spark.range(1).selectExpr("""'{"myJSONKey" : {"myJSONValue" : [1,2,3]}}' )

get_json_object # JSON객체 인라인 쿼리로 조회
json_tuple # 중첩 없는 단일 수준 JSON 객체
to_json #StructType -> JSON문자열
from_json #JSON 문자열 -> 객체
```

### 6.11 사용자 정의 함수

- 스파크의 가장 강력한 기능 중 하나
- 하나 이상의 컬럼을 입력으로 받고 반환
- 레코드별로 데이터 처리
- UDF는 DataFrame에서만 사용 가능
    - SQL함수로 등록하면 모든 곳에서 사용 가능
- UDF 처리 과정

![Untitled](https://user-images.githubusercontent.com/70019911/128107139-687e8842-e3bc-4517-afb4-038751113ec6.png)

[출처]([https://chioni.github.io/posts/scalaudf/](https://chioni.github.io/posts/scalaudf/))

- 파이썬으로 UDF를 작성하면 큰 부하가 발생해 자바나 스칼라로 작성하는 게 좋다.

```jsx
//숫자를 3제곱하는 함수
def power3(double_value):
	return double_value**3

// 입력값을 특정 데이터 타입으로 강제하고 null 값을 입력하지 못하게 해야 함

//함수 등록
val power3udf = udf(power3)

//사용
udfExampleDF.select(power3udf(col("num"))).show()

//SQL함수로 등록
spark.udf.register("power3py", power3, DoubleType())
```

### 6.12 Hive UDF

- 하이브 문법을 사용한 UDF도 가능
    - SparkSession을 생성할 때 SparkSession.builder().enableHiveSupport()를 명시해서 하이브 지원 기능을 활성화 해야 함
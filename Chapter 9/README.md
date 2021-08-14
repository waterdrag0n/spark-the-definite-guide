# Chapter 9: 데이터소스

### 9.1 데이터소스 API의 구조

- 9.1.1 읽기 API의 구조

```jsx
DataFrameReader.format(...).option("key", "value").schema(...).load()

format : 선택적으로 사용, 기본값=파케이
option : 데이터를 읽는 방법에 대한 파라미터 설정
schema : 데이터 소스에서 스키마를 제공하거나 
				 스키마 추론 기능을 사용하려는 경우에 선택적으로 사용.
```

- 9.1.2 데이터 읽기의 기초
    - 데이터를 읽을 때는 DataFrameReader 사용
    - 읽기 모드
        - 형식에 맞지 않는 데이터를 만났을 때 동작 방식을 지정하는 옵션


### 스파크의 읽기 모드

|읽기 모드|설명|
|------|---|
|permissive|오류 레코드의 모든 필드를 null로 설정하고 모든 오류 레코드를 corrupt_record라는 문자열 컬럼에 기록|
|dropMalformed|형식에 맞지 않는 레코드가 포함된 로우 제거|
|failFast|형식에 맞지 않는 레코드를 만나면 즉시 종료|

- 9.1.3 쓰기 API 구조

```jsx
DataFrameWriter.format(...).option(...)
.paartitionBy(...).bucketBy(...).sortBy(...).save()
```

- 9.1.4 데이터 쓰기의 기초
    - 데이터 읽기와 유사하며, DataFrameWriter 사용
    - 저장 모드
        - 스파크가 지정된 위치에서 동일한 파일이 발견됐을 때의 동작 방식

### 스파크의 저장 모드

|저장 모드|설명|
|------|---|
|append|해당 경로에 이미 존재하는 파일 목록에 결과 파일 추가|
|overwrite|이미 존재하는 데이터 완전히 덮어씀|
|errorIfExists|오류를 발생시키면서 쓰기 작업 실패 (기본값)|
|ignore|아무런 처리도 하지 않음|

### 9.2 CSV 파일

- 구조적으로 보이지만 운영 환경에서는 어떤 내용, 구조로 되어 있는지를 확인할 수 없어 까다로운 파일 포맷.
- 9.2.1 CSV 옵션
    - 스파크 완벽 가이드 표 9-3 참고.

```python
# 9.2.2 CSV 파일 읽기
# DataFrame 정의 시점에 문제가 있어도 스파크는 오류를 감지하지 못하고 잡 실행 시점에 오류가 발생하게 됨

# 우선 CSV용 DataFrameReader 생성
spark.read.format('csv')

# 스키마와 읽기 모드 지정
spark.read.format('csv')
			.option('header'. 'true')
			.option('mode', 'FAILFAST')
			.option('inferSchema', 'true')
			.load('some/path/to/file.csv')

csvFile = spark.read.format('csv')
			.option('header'. 'true')
			.option('mode', 'FAILFAST')
			.option('inferSchema', 'true')
			.load('some/path/to/file.csv')

# 9.2.3 CSV 파일 쓰기
# csv => tsv
csvFile.write.format('csv').mode('overwrite')
.option('sep', '\t').save('/tmp/my-tsv-file.tsv')
```

### 9.3 JSON 파일

- 스파크에서 JSON 파일을 사용할 때 줄로 구분된 JSON을 기본적으로 사용.
- multiLine 옵션으로 줄로 구분된 방식과 여러 줄로 구성된 방식 선택
- 9.3.1 JSON 옵션
    - 스파크 완벽 가이드 표 9-4

```python
# 9.3.2 JSON 파일 읽기
spark.read.format('json')
			.option('mode', 'FAILFAST')
			.schema(myManualSchema)
			.load('some/path/to/file.json')
			.show()

# 9.3.3 JSON 파일 쓰기
csvFile.write.format('json').mode('overwrite')
.save('/tmp/my-json-file.json')
```

### 9.4 파케이 파일

- 파케이 : 다양한 스토리지 최적화 기술을 제공하는 오픈소스로 만들어진 컬럼 기반의 데이터 저장 방식. 특히 분석 워크로드에 최적화.
- 저장소 공간을 절약하고 전체 파일 대신 개별 컬럼을 읽을 수 있으며, 컬럼 기반 압축 기능 제공.
- 데이터를 저장할 때 자체 스키마를 이용해 저장하기 때문에 옵션이 거의 없음.


### 파케이 옵션

|읽기/쓰기|키|사용 가능한 값|기본값|설명|
|------|---|---|---|---|
|모두|compression or codec|none, uncompressed, bzip2, deflate, gzip, lz4, snappy|none|압축 코덱 정의|
|읽기|mergeSchema|true, false|spark.sql.parquet.mergeShema 속성의 설정값|동일한 테이블이나 폴더에 신규 추가된 파케이 파일에 컬럼을 점진적으로 추가|

```python
# 9.4.1 파케이 파일 읽기
spark.read.format('parquet')
			.load('some/path/to/file.parquet').show()

# 9.4.2 파케이 파일 쓰기
csvFile.write.format('parquet').mode('overwrite')
.save('/tmp/my-parquet-file.parquet')
```

### 9.5 ORC 파일

- ORC: 하둡 워크로드를 위해 설계된 자기기술적이며 데이터 타입을 인식할 수 있는 컬럼 기반 파일 포맷
- 대규모 스트리밍에 최적화, 필요한 로우를 신속하게 찾아내는 기능

```python
# 9.5.1 ORC 파일 읽기
spark.read.format('orc')
			.load('some/path/to/file.orc').show()

# 9.5.2 ORC 파일 쓰기
csvFile.write.format('orc').mode('overwrite')
.save('/tmp/my-orc-file.orc')
```

### 9.6 SQL 데이터베이스

```python
# 9.6.1 SQL 데이터베이스 읽기
driver = 'org.sqlite.JDBC'
pth = '/data/path/to/file/my-sqlite.db'
url = 'jdbc:sqlite:' + path
tablename = 'flight_info'

imoprt java.sql.DriverManager

val connection = DriverManager.getConnection(url)
connection.isClosed()
connection.close()

dbDataFrame = spark.read.format('jdbc')
			.option('url', url).option('dbtable', tablename)
			.option('driver', driver) ,.load()

# 9.6.2 쿼리 푸시다운
# df를 만들기 전에 db 자체에서 필터링할 수 있음
# 필터를 명시하면 해당 필터의 처리를 db로 위임(push down)
dbDataFrame.filter("DEST_COUNTRY_NAME IN ('Anguilla', 'Sweden')").explain()

# 전체 쿼리를 db에 전달해 결과를 df로 받아야 하는 경우
pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info) AS flight_info"""
dbDataFrame = spark.read.format("jdbc").option('url',url)
.option('dbtable', pushdownQuery).option('driver',driver).load()

# 데이터베이스 병렬로 읽기
# 스파크는 분할 가능성에 따라 여러 파일을 읽어 
# 하나의 파티션으로 만들거나 여러 파티션을 하나의 파일로 만드는 기본 알고리즘을 가지고 있음.
# 이를 SQL db에서 사용하려면 수동 설정 필요
.option('numPartitions',10)

# 슬라이딩 윈도우 기반 파티셔닝
# 조건절을 기반으로 분할하는 방법
spark.read.jdbc(url, tablename, column=colName, properties=props,
								lowerBound=lowerBound, upperBound=upperBound,
								numPartitions=numPartitions).count()

# 9.6.3 SQL 데이터베이스 쓰기
csvFile.write.jdbc(path,tablename, mode="overwrite", properties=props)
```

### 9.7 텍스트 파일

```python
# 9.7.1 텍스트 파일 읽기
spark.read.textFile('path').selectExpr("split(value ',') as rows').show()

# 9.4.2 텍스트 파일 쓰기
csvFile.select('DEST_COUNTRY_NAME', 'count').write.partitionBy('count').text('path')
```

### 9.8 고급 I/O 개념

- 9.8.1 분할 가능한 파일 타입과 압축 방식
    - 특정 파일 포맷은 기본적으로 분할 지원
        - 쿼리에 필요한 부분만 읽을 수 있음
    - 모든 압축 방식이 분할 압축을 지원하지 않음
        - 추천 파일 포맷과 압축 방식 : 파케이 & GZIP
- 9.8.2 병렬로 데이터 읽기
    - 여러 익스큐터가 같은 파일을 읽는 것은 불가능하지만 여러 파일을 동시에 읽을 수 있음
    - 다수의 파일이 존재하는 폴더를 읽을 때 폴더의 개별 파일은 df의 파티션
    - 병렬로 파일을 읽게 됨
- 9.8.3 병렬로 데이터 쓰기
    - 파일, 데이터 수는 쓰는 시점에 df가 가진 파티션 수에 따라 다름
    - 기본적으로 데이터 파티션 당 하나의 파일 작성
    - 파티셔닝
        - 어떤 데이터를 어디에 저장할 것인지 제어하는 기능
        - 필요한 컬럼의 데이터만 읽을 수 있음
        - 필터링을 자주 사용하는 테이블을가진 경우에 효과적
    - 버켓팅
        - 동일한 버켓ID를 가진 데이터가 하나의 물리적 파티션에 모여있기 때문에 데이터를 읽을 때 셔플을 피할 수 있음
        - 이후의 사용 방식에 맞춰 사전에 파티셔닝
- 9.8.4 복합 데이터 유형 쓰기
    - 다양한 자체 데이터 타입이 스파크에서는 잘 동작하지만 모든 데이터 파일 포맷에 적합한 것은 아님
        - CSV는 복합 데이터 파일을 지원하지 않지만 파케이 ORC는 지원
- 9.8.5 파일 크기 관리
    - 작은 파일을 많이 생성하면 메타데이터에 부하 발생
    - 스파크 2.2에서 자동으로 파일 크기를 제어하는 방법 도입
    - maxRecordsPerFile 옵션에 파일당 레코드 수를 지정
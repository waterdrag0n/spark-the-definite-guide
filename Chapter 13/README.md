# Chapter 13: RDD 고급 개념

### 13.1 키-값 형태의 기초(키-값 형태의 RDD)

```python
# RDD를 키-값 형태로 다루는 다양한 메서드 - <연산명>ByKey 형태
# 메서드 이름에 ByKey가 있으면 PairRDD 타입만 사용 가능
# RDD에 맵 연산을 수행해서 키-값 구조로 만들어 PairRDD를 만들 수 있음
words.map(lambda word: (word.lower(), 1))

# 13.1.1 keyBy
# 키 생성
keyword = words.keyBy(lambda word: word.lower()[0])

# 13.1.2 값 매핑하기
keyword.mapValues(lambda word: word.upper()).collect()
keyword.flatMapValues(lambda word: word.upper()).collect()

# 13.1.3 키와 값 추출하기
keyword.keys().collect()
keyword.values().collect()

# 13.1.4 lookup
# 키에 관한 결과
keyword.lookup('S')

# 13.1.5 sampleByKey
# 키를 기반으로 RDD 샘플 생성
# 선택에 따라 비복원추출도 가능
import random

distinctChars = words.flatMap(lambda word: list(word.lower())).distinct().collect()
sampleMap = dict(map(lambda c: (c, random.random()), distinctChars))
words.map(lambda word: (word.lower()[0], word)).sampleByKey(True, sampleMap, 6).collect()
```

### 13.2 집계

```python
chars = words.flatMap(lambda word: word.lower())
KVcharacters = chars.map(lambda letter: (letter, 1))

def maxFunc(left, right):
	return max(left, right)
def addFunc(left, right):
	return left+right

nums = sc.parallelize(range(1, 31), 5)

# 13.2.1 countByKey
# 각 키의 아이템 수를 구하고 로컬 맵으로 결과를 수집
# 스칼라나 자바를 사용하면 제한시간과 신뢰도를 설정해 근사치를 구할 수 있음
KVcharacters.countByKey()

# 13.2.2 집계 연산 구현 방식 이해하기
# groupByKey
KVcharacters.groupByKey().map(lambda row: (row[0], reduce(addFunc, row[1]))).collect()
# python3 사용 시 reduce import
# map 사용 시 모든 값을 메모리로 읽기 때문에 out of memory 우려

# reduceByKey
KVcharacters.reduceByKey(addFunc).collect()

# 13.2.3 기타 집계 메서드

# aggregate
# null이나 집계의 시작값이 필요하며
# 두 가지 함수 (파티션 내에서 수행되는 함수, 모든 파티션에 수행되는 함수)
nums.aggregate(0, maxFunc, addFunc)
# 드라이버에서 최종 집계를 수행하기 때문에 성능에 영향

# treeAggregate
# 드라이버 전에 익스큐터끼리 트리를 형성해 집계 처리의 일부 하위 과정을 push down으로 수행
nums.treeAggregate(0, maxFunc, addFunc, depth)

# aggregateByKey
# aggregate와 동일하지만 파티션 대신 키를 기준으로 수행
KVcharacters.aggregateByKey(0, maxFunc, addFunc).collect()

# combineByKey
# 집계함수 대신 컴바이너 사용
# 컴바이너 - 키를 기준으로 연산 수행, 파타미터로 사용된 함수에 따라 값 병합, 결괏값 병합 후 반환
def valToCombiner(value):
	return [value]
def mergeValuesFunc(vals, valToAppend):
	vals.append(valToAppend)
	return vals
def mergeCombinerFunc(val1, val2):
	return val1+val2
outputPartitions = 6

KVcharacters\
	.combineByKey(valToCombiner, mergeValuesFunc, mergeCombinerFunc, outputPartitions)\
	.collect()

# foldByKey
# 결합 함수와 항등원인 제로값을 이용해 각 키의 값을 병합
# 제로값은 결과에 여러번 사용될 수 있지만 결과를 변경할 수 없음
KVcharacters.foldByKey(0)(addFunc).collect()
```

### 13.3 cogroup

- scala 최대 3개, 파이썬 최대 2개의 RDD 그룹화 가능
- RDD에 대한 그룹 기반의 조인 수행

```python
import random

distinctChars = words.flatMap(lambda word: word.lower()).distinct()
charRDD = distinctChars.map(lambda c: (c, random.random()))
charRDD2 = distinctChars.map(lambda c: (c, random.random()))

charRDD.cogroup(charRDD2).take(5)
```

### 13.4 조인

```python
# 13.4.1 내부 조인
keyedChars = distinctChars.map(lambda c: (c, random.random()))
outputPartitions = 10

KVcharacters.join(keyedChars).count()
KVcharacters.join(keyedChars, outputPartitions).count()

# fullOuterJoin: 외부 조인
# leftOuterJoin: 왼쪽 외부 조인
# leftOuterJoin: 오른쪽 외부 조인
# cartesian: 교차 조인 (위험)

# 13.4.2 zip
# 진짜 조인은 아니고 두 개의 RDD를 연결
numRange = sc.parallelize(range(10), 2)
words.zip(numRange).collect()
```

### 13.5 파티션 제어하기

```python
# 13.5.1 coalesce
# 파티션을 재분배할 때 데이터 셔플을 방지하기 위해 동일한 워커에 존재하는 파티션을 합치는 메서드
words.coalesce(1).getNumPartitions() # 1

# 13.5.2 repartition
# 파티션 수를 늘리거나 줄일 수 있음. 셔플 발생
words.repartition(10) # 10개 파티션 생성

# 13.5.3 repartitionAndSortWithinPartitions
# 파티션 재분배, 정렬 방식 지정

# 13.5.4 사용자 정의 파티셔닝
# RDD를 사용하는 가장 큰 이유 중 하나
# 목표: 데이터 치우침(skew)와 같은 문제를 피하고자 클러스터 전체에 걸쳐 데이터 균등 분배
# 구조적 API로 RDD를 얻고 사용자 정의 파티셔너를 적용한 다음 다시 DataFrame이나 Dataset으로 변경
df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(path)
rdd = df.coalesce(10).rdd
df.printSchema()

# 이산형 연속형 값을 다룰 때 사용
# scala
import org.apache.spark.HashPartitioner

rdd.map(r => r(6)).take(5).foreach(println)
val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf([Int].toDouble)
keyedRDD.partitionBy(new HashPartitioner(10)).take(10)

# 병렬성을 개선하고 실행 과정에서 out of memory 방지를 위해 키를 최대한 분할해야 함
def partitionFunc(key):
	import random
	if key == 17850 or key == 12583:
		return 0
	else:
		return random.randint(1,2)

keyedRDD = rdd.keyBy(lambda row: row[6])

keyedRDD\
	.partitionBy(3, partitionFunc)\
	.map(lambda x: x[0])\
	.glom()\
	.map(lambda x: len(set(x)))\
	.take(5)
```

### 13.6 사용자 정의 직렬화

- Kyro 직렬화
    - Java 진영의 빠르고 효율적인 바이너리 객체 그래프 직렬화 프레임워크
    - 고속, 경량의 사용하기 쉬운 API
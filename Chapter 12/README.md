# Chapter 12 : RDD

- 대부분의 경우 구조적 API를 사용하는 것이 좋지만, 모든 상황에서 고수준 API로 처리가 가능한 것은 아님
- 이런 경우 스파크의 저수준 API, RDD, SparkContext, 분산형 공유 변수(accumulator, broadcast variable) 사용

### 12.1 저수준 API란

- 두 종류의 저수준 API
    - 분산 데이터 처리를 위한 RDD
    - 분산형 공유 변수를 배포하고 다루기 위한 API
        - 브로드캐스트 변수, 어큐뮬레이터
- 12.1.1 저수준 API는 언제 사용할까
    - 고수준 API에서 제공하지 않는 기능이 필요한 경우
        - e.g 클러스터의 물리적 데이터의 배치를 아주 세밀하게 제어해야 하는 상황
    - RDD를 사용해 개발된 기존 코드를 유지해야 하는 경우
    - 사용자가 정의한 공유 변수를 다뤄야 하는 경우
- 12.1.2 저수준 API는 어떻게 사용할까
    - SparkContext는 저수준 API를 사용하는 진입 지점
    - SparkSession을 이용해 SparkContext에 접근

    ```scala
    spark.sparkContext
    ```

### 12.2 RDD 개요

- RDD는 스파크 2 버전부터는 잘 사용하지 않지만 모든 DataFrame, Dataset 코드는 RDD로 컴파일 되기 때문에 이해하는 것이 중요
- RDD - 불변성, 병렬로 처리할 수 있는 파티셔닝된 레코드의 모음
- df의 레코드는 스키마를 알고 있는 필드로 구성된 구조화된 로우인 반면 RDD의 레코드는 프로그래머가 선택하는 자바, 스칼라, 파이썬의 객체일 뿐
    - 따라서 완벽하게 제어 가능
    - 사용자가 원하는 포맷으로 원하는 모든 데이터 저장
    - 강력한 제어권을 가지지만, 값을 다루는 모든 과정을 수동으로 정의해야 함
- 12.2.1 RDD 유형
    - RDD는 df API에서 최적화된 물리적 실행 계획을 만드는 데 대부분 사용
    - RDD 유형
        1. 제네릭 RDD
        2. 키-값 RDD
            - 키 기반 집계 가능
    - RDD 속성 (내부적으로 구분)
        1. 파티션의 목록
        2. 각 조각을 연산하는 함수
        3. 다른 RDD와의 의존성 목록
        4. 부가적으로 키-값 RDD를 위한 Partitioner
        5. 부가적으로 각 조각을 연산하기 위한 기본 위치 목록 (e.g HDFS 파일의 블록 위치)
        - 이 속성을 통해 스파크의 모든 처리 방식 결정
    - RDD는 트랜스포메이션과 액션으로 이어지는 스파크 프로그래밍 패러다임을 따르지만, 로우가 없고 개별 레코드는 객체일 뿐이기 때문에 함수를 사용할 수 있는 구조적 API와 달리 수동으로 처리해야 함
    - 자바나 스칼라로 RDD를 사용하면 성능 손실이 크지 않지만 파이썬은 성능 손실이 큼.
- 12.2.2 RDD는 언제 사용할까
    - 물리적으로 분산된 데이터 (자체적으로 구성한 데이터 파티셔닝)에 세부적인 제어를 할 때 사용

### 12.3 RDD 생성하기

```python
# 12.3.1 DataFrame, Dataset으로 RDD 생성하기
spark.range(10).rdd

# 위 데이터를 처리하려면 Row 객체를 올바른 데이터 타입으로 변환하거나 Row 객체에서 값 추출
spark.range(10).toDF('id').rdd.map(lambda row:row[0])
# Row 타입 RDD 반환

# RDD로 DF, Dataset 생성
spark.range(10).rdd.toDF()

# 12.3.2 로컬 컬렉션으로 RDD 생성하기
# 컬렉션 객체를 RDD로 만들려면 SparkSession 안의 sparkContext의 parallelize 호출
# 단일 노드의 컬렉션을 병렬 컬렉션으로 전환, 파티션 개수 지정

myCollection = someString.split(" ")
words = spark.sparkContext.parallelize(myCollection, 2)

words.setName("myWords")
words.name() # myWords

# 12.3.3.데이터소스로 RDD 생성하기
spark.sparkContext.textFile('path')

# 텍스트 파일 전체를 레코드로 읽을 때
# 파일명: RDD키, 텍스트 파일 값: RDD값
spark.sparkContext.wholeTextFile('path')
```

### 12.4 RDD 다루기

- DataFrame 다루는 방식과 유사하지만, 함수 부족
- 필터, 맵, 함수, 집계 등을 직접 정의해야 함

### 12.5 트랜스포메이션

```python
# 12.5.1 distinct
# RDD에서 중복된 데이터 제거
words.distinct().count()

# 12.5.2 filter
def startsWithS(individual):
	return individual.startWith('S')
words.filter(lambda word: startsWithS(word)).collect()

# 12.5.3 map
words2 = words.map(lambda word: (word, word[0], word.startswith('S')))
words2.filter(lambda record: record[2]).take(5)

# flatMap
# 단일 로우를 여러 로우로 변환하는 경우
words.flatMap(lambda word: list(word)).take(5)

# 12.5.4 sortBy
words.sortBy(lambda word: len(word) * -1).take(2)

# 12.5.5 randomSplit
# RDD를 임의로 분할해 RDD 배열을 만들 때 사용
# 가준치와 random seed로 구성된 배열을 파라미터로 제공
fiftyFiftySplit = words.randomSplit([0.5, 0.5])
```

### 12.6 액션

```python
# 12.6.1 reduce
# 모든 값을 하나로
spark.sparkContext.parallelize(range(1,21)).reduce(lambda x,y: x+y)

# ex 가장 긴 단어를 찾는 예제
def wordLengthReducer(leftWord, rightWord):
	if len(leftWord) > len(rightWord):
		return leftWord
	else:
		return rightWord

words.reduce(wordLengthReducer)

# 12.6.2 count
words.count() # 전체 로우 수

# countApprox
# count의 근사치를 제한 시간 내에 계산
words.countApprox(timeoutMilliseconds, confidence)

# countApproxDistinct
# 1번 구현 - 상대정확도를 파타미터로 사용
words.countApproxDistinct(0.05)
# 2번 구현 - 일반 데이터를 위한 파타미터, 희소 표현을 위한 파타미터 (정밀도, 희소 정밀도)
words.countApproxDistinct(4, 10)

# countByValue
# RDD값의 개수를 구해서 익스큐터의 결과를 드라이버 메모리에 모두 적재
# 결과가 작은 경우에만 사용
words.countByValue()

# countByValueApprox
words.countByValueApprox(제한시간, 오차율)

# 12.6.3 first
# 첫 값 반환
words.first()

# 12.6.4 max와 min
spark.sparkContext.parellelize(1 to 20).max()
spark.sparkContext.parellelize(1 to 20).min()

# 12.6.5 take
# 하나의 파티션 스캔 - 결과 수를 이용해 필요한 추가 파티션 수 예측
words.take(5)
words.takeOrdered(5)
words.top(5)
words.takeSample(withReplacement, numToTake, randomSeed)
```

### 12.7 파일 저장하기

- RDD를 사용하면 일반적인 의미의 데이터소스에 저장할 수 없음
- 각 파티션의 내용을 저장하려면 전체 파티션을 순회하면서 외부 DB에 저장해야 함

```python
# 12.7.1 saveAsTextFile
words.saveAsTextFile('path')
# 압축 코덱 설정 시
import org.apache.hadoop.io.compress.BZip2Codec
words.saveAsTextFile('path', classOf(BZip2Codec))

# 12.7.2 시퀀스 파일
# 시퀀스 파일 : 바이너리 키-값으로 구성된 플랫 파일. 맵리듀스의 입출력 포맷
words.saveAsObjectFile('path')
```

### 12.8 캐싱

```python
# RDD를 캐시하거나 저장할 수 있음
words.cache()

# 저장소 수준 조회
words.getStorageLevel()
```

### 12.9 체크포인팅

- DataFrame에는 없는 기능
- RDD를 디스크에 저장하는 것
- 나중에 저장된 RDD를 참조할 때 다시 계산해서 RDD를 생성하지 않고 디스크에 저장된 중간 결과를 참조

```python
spark.sparkContext.setCheckpointDir(path)
words.checkpoint()
```

### 12.10 RDD를 시스템 명령으로 전송하기

```python
# pipe 메서드
# 파이핑 요소로 생성된 RDD를 외부 프로세스로 전달
# 파티션마다 한 번씩 처리해 RDD 생성
words.pipe("wc -l").collect()

# 12.10.1 mapPartitions
# map함수에서 반환하는 RDD는 MapPartitionsRDD
# 개별 파티션에 대해 map 수행
words.mapPartitions(lambda part: [1]).sum()

# mapPartitionsWithIndex
def indexedFunc(partitionIndex, withinParIterator):
	return ["partition: {} => {}".format(partitionIndex, x)
		for x in withinPartIterator]

words.mapPartitionsWithIndex(indexedFunc).collect()

# 12.10.2 foreachPartition
# 모든 데이터를 순회할 뿐 결과 반환 x
words.foreachPartition { iter =>
	import java.io._
	import scala.util.Random
	
	val randomFileName = new Random().nextInt()
	val pw = new PrintWriter(new File(path.txt))
	while(iter.hasNext) {
		pw.write(iter.next())
	}
	pw.close()
}

# 12.10.3 glom
# 모든 파티션을 배열로 변환
# 파티션이 크거나 개수가 많으면 드라이버가 비정상적으로 종료될 수 있어 주의
spark.sparkContext.parallelize(["Hello", "World"], 2).glom().collect()
# [['Hello'], ['World']]
```
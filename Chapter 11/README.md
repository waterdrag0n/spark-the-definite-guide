# Chapter 11: Dataset

- Dataset: 구조적 API의 기존 데이터 타입
- Dataset은 스칼라와 자바에서만 사용 가능 ( JVM 지원)
- 스파크는 StringType, BigIntType, StructType과 같은 다양한 데이터 타입을 지원하는데 다른 언어의 String, Integer, Double과 같은 다른 언어의 타입을 스파크 데이터 타입으로 매핑 가능
- 인코더: 도메인별 특정 객체를 스파크 내부 데이터 타입으로 변환
- 데이터셋을 사용하여 접근하면 Row 포맷이 아닌 사용자 정의 데이터 타입으로 변환
    - 느려지지만 유연성 제공

### 11.1 Dataset을 사용할 시기

- 성능이 떨어짐에도 데이터셋을 사용해야 하는 두 가지 이유
    1. df 기능만으로는 수행할 연산을 표현할 수 없는 경우
        - 복잡한 비즈니스 로직을 SQL이나 df 대신 단일 함수로 인코딩해야 하는 경우
    2. 성능 저하를 감수하더라도 타입 안정성을 가진 데이터 타입을 사용하고 싶은 경우
        - 두 문자열을 뺄셈하는 것처럼 타입이 유효하지 않은 작업은 런타임이 아닌 컴파일 타임에 오류 발생
            - 런타임에러: 컴파일 되어 실행되는 중에 발생하는 에러
            - 컴파일타임에러: 컴파일 되는 중에 발생하는 에러
- 로컬과 분산 환경의 워크로드에서 재사용

### 11.2 Dataset 생성

- 11.2.1 자바: Encoders
    - 데이터 타입 클래스 정의 후 DataFrame(Dataset<Row>)에 지정해 인코딩

```java
public class Flight implements Serializable {
	String DEST_COUNTRY_NAME;
	String ORIGIN_COUNTRY_NAME;
	Long DEST_COUNTRY_NAME;
}

Dataset<Flight> flights = spark.read
.parquet('path')
.as(Encoders.bean(Flight.class));
```

- 11.2.2 스칼라: 케이스 클래스
    - 스칼라 case class 구문으로 데이터 타입 정의
    - 케이스 클래스
        - 정규 클래스임
        - 불변성
            - 객체들이 언제 어디서 변경되었는지 추적할 필요 없음
        - 패턴 매칭으로 분해 가능
            - 로직 분기를 단순화해 버그를 줄이고 가독성을 좋게 만듦
        - 참조값 대신 클래스 구조를 기반으로 비교
        - 사용하기 쉽고 다루기 편함

    ```java
    case class Flight(DEST_COUNTRY_NAME:String, ORIGIN_COUNTRY_NAME:String, count:BigInt)

    val flightsDF = spark.read.parquet('path')
    val flights = flightsDF.as[Flight] 
    ```

### 11.3 액션

```java
flights.show()

// 케이스 클래스에 실제로 접근할 때 어떠한 데이터 타입도 필요하지 않음
// 속성명을 지정하면 맞는 값과 데이터 타입 모두 반환
flights.first.DEST_COUNTRY_NAME // United States
```

### 11.4 트랜스포메이션

- Dataset의 트랜스포메이션은 df의 트랜스포메이션과 동일
- Dataset을 사용하면 더 복잡하고 강력한 타입으로 사용 가능

```scala
// 11.4.1 필터링
def originIsDestination(flight_row: Flight): Boolean = {
	return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
}
flights.filter(flight_row -> originIsDestination(flight_row)).first()

// 11.4.2 매핑
val destinations = flights.map(f -> f.DEST_COUNTRY_NAME)
// 드라이버는 결괏값을 모아 문자열 타입의 배열로 반환
val localDestinations = destinations.take(5)
// DataFrame을 사용하면 코드 생성 기능과 같은 장점을 얻어 매핑보다 추천
```

### 11.5 조인

```scala
// Dataset은 joinWith처럼 정교한 메서드 제공
// joinWith: co-group (RDD)아 비슷하고 Dataset 안쪽에 다은 두 개의 중첩된 Dataset으로 구성
val flights = flights.joinWith(flightsMeta, 
flights.col("count") === flightsMeta.col("count))

// 일반적인 join도 잘 동작
// 하지만 df를 반환하기 때문에 JVM 데이터 타입 정보는 사라짐
val flights2 = flights.join(flightsMeta, Seq('count'))

// JVM 데이터 타입 정보를 유지하려면
val flights2 = flights.join(flightsMeta.toDF(), Seq('count'))
```

### 11.6 그룹화와 집계

```scala
// groupBy, rollup, cube 모두 사용 가능
// 하지만 역시 df를 반환하여 데이터 타입 정보는 사라짐
flights.groupBy('DEST_COUNTRY_NAME').count()

// 유지 방법
// 성능 차이 발생
flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()

// 새로운 처리 방법을 생성해 그룹 축소
flights.groupByKey(x => x.DEST_COUNTRY_NAME).count()
.reduceGroups((l, r) => sum2(l, r)).take(5)
```
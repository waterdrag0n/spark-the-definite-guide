# Chapter 7: 집계 연산

### 집계

- 빅데이터 분석의 초석
- 집계를 수행하려면 키나 그룹을 지정하고 하나 이상의 컬럼을 변환하는 방법을 지정하는 '집계 함수' 사용

### 7.1 집계 함수

- 7.1.1 count
    - count 사용법 (1)
        - 특정 컬럼을 지정하여 사용 (null 불포함)
        - count(*) (null 포함) || count(1)
    - 전체 로우 수 카운트
- 7.1.2 countDistinct
    - 고유 레코드 수
    - 개별 컬럼 처리에 적합
- 7.1.3 approx_count_distinct
    - 대규모 데이터에서 정확한 수보다 근사치로도 유의미할 때 사용
    - 인자로 추정오류율을 받는데 큰 오류율을 설정할수록 속도가 빨라짐
- 7.1.4 first 와 last
    - 첫/마지막 값 얻을 때 (로우 기반)
- 7.1.5 min과 max
- 7.1.6 sum
    - 특정 컬럼의 모든 값 합산
- 7.1.7 sumDistinct
    - 고윳값 합산
- 7.1.8 avg
    - distinct를 사용하면 고윳값의 평균도 구할 수 있음
- 7.1.9 분산과 표준편차
    - spark는 표본표준편차와 모표준편차를 모두 지원해서 유의해야 함.
    - 표본표준편차 - variance, stddev
    - 모표준편차 - var_pop, stddev_pop
- 7.1.10 비대칭도와 첨도
    - 데이터의 변곡점을 측정하는 방법 : 비대칭도, 첨도
        - 비대칭도 : 실숫값 확률변수의 확률분포 비대칭정을 나타내는 지표
        - 첨도 : 확률분포의 뾰족한 정도를 나타내는 지표.
    - 확률변수를 확률분포로 데이터 모델링할 때 중요
    - skewness, kurtosis
- 7.1.11 공분산과 상관관계
    - 두 컬럼 사이의 영향도 비교
    - 공분산(covar_samp, covar_pop), 상관관계(corr)
- 7.1.12 복합 데이터 타입의 집계
    - 특정 컬럼의 값을 수집하거나 고윳값만 수집한 복합 데이터 타입을 사용자 정의함수로 집계 가능

### 7.2 그룹화

- 데이터 그룹 기반의 집계를 수행하기 위해 카테고리형 데이터 사용
- 그룹 기반 집계를 그룹화를 통해
- 그룹화 단계 1) RelationGroupedDataset 반환, 2) DataFrame 반환
- 7.2.1 표현식을 이용한 그룹화
    - count는 메서드와 함수로 사용할 수 있는데 함수로 사용하는 것이 좋음
    - count는 select 구문에 표현식으로 지정하기보다 agg 메서드를 사용하는 것이 좋음

    ```jsx
    df.groupBy("InvoiceNo").agg(count("Quantity") \
    .alias("quan"), expr("count(Quantity)")).show()
    ```

- 7.2.2 맵을 이용한 그룹화
    - map 타입을 이용해 트랜스포메이션 정의
    - 수행할 집계함수를 한 줄로 작성하면 여러 컬럼명 재사용 가능

### 7.3 윈도우 함수

- 윈도우 함수 : 데이터의 특정 윈도우를 대상으로 연산 수행
    - 행과 행 간의 관계를 쉽게 정의하기 위해
- 데이터의 윈도우 : 현재 데이터에 대한 참조를 사용해 정의
- groupby vs window
    - groupby는 모든 로우 레코드가 단일 그룹으로 이동
    - 윈도우 함수는 프레임에 입력되는 모든 로우에 대한 결괏값을 계산
- 스파크에서 지원하는 윈도우 함수
    - 랭크 함수 (ranking function)
    - 분석 함수 (analytic function)
    - 집계 함수 (aggregate function)

```python
# 첫 단계 : 윈도우 명세 만들기
windowSpec = Window.partitionBy("CustomerId", "date") # partitionBy: 그룹을 어떻게 나눌지
		.orderBy(desc("Quantity"))
		.rowsBetween(Window.unboundedPreceding. Window.currentRow) 
		# 입력된 로우의 참조를 기반으로 프레임에 로우가 포함될 수 있는지 결정

# 집계 : 컬럼명, 표현식 전달, 정의한 윈도우 명세 사용
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

# 순위
dense_rank().over(windowSpec) # 중복 순위 상관 없음
rank().over(windowSpec) # 중복 순위만큼 건너뜀
```

### 7.4 그룹화 셋

- 여러 그룹에 걸쳐 집계하기 위해 그룹화 셋 사용 (SQL만 가능)

```python
SELECT CustomerId, stockCode sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode
ORDER BY CustomerId DESC, stockCode DESC

SELECT CustomerId, stockCode sum(Quantity) FROM dfNoNull
GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
ORDER BY CustomerId DESC, stockCode DESC
```

- DataFrame에서는 rollup과 cube 사용
- 7.4.1 롤업
    - rollup을 사용하면 맨 위 로우에 전체의 합계를 확인할 수 있음
    - 합계 컬럼 외에 다른 컬럼은 null로 표시됨. 모두 null은 전체 합계
- 7.4.2 큐브
    - 롤업을 고차원적으로 사용할 수 있게
    - 요소들을 계층적으로 다루는 대신 모든 차원에 대해 동일한 작업 수행
- 7.4.3 그룹화 메타데이터
    - 집계 수준에 따라 필터링하기 위해 집계 수준을 조회
    - grouping_id를 통해 확인할 수 있음

    [그룹화 ID의 의미]
|그룹화ID|설명|
|-|-|
|3|가장 높은 계층의 집계 결과에서 나타남. 총 수량 제공|
|2|컬럼2에 따른 총 결과|
|1|컬럼1에 따른 총 결과|
|0|컬럼1,2에 따른 총 결과|

- 7.4.4 피벗
    - 로우를 컬럼으로 변환
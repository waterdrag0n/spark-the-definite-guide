# Chapter 8: 조인

### 8.1 조인 표현식

- 왼쪽과 오른쪽 데이터셋에 있는 하나 이상의 키 값을 비교해서 결합 여부를 결정
- equi-join : 왼쪽과 오른쪽 데이터셋에 지정된 키가 동일한지 비교
    - 일치하면 결합
    - 일치하지 않는 로우는 조인에 포함하지 않음
- 배열 타입과 같은 복합 데이터 타입도 조인 가능

### 8.2 조인 타입

- 데이터셋에 어떤 데이터가 있어야 하는가

```python
# 8.3 내부 조인
# 데이터프레임이나 테이블에 존재하는 키를 평가하여 true로 평가되는 로우만 결합
# 양쪽 모두에 키가 존재하지 않으면 결과에서 볼 수 없다.
joinExpr = person["graduate_program"] == graduateProgram['id']
person.join(graduateProgram, joinExpr).show()

# 더 명확하게 하려면
joinType = 'inner'
person.join(graduateProgram, joinExpr, joinType).show()

# 8.4 외부 조인
# false로 평가되는 로우도 포함하여 조인
# 양쪽 모두 일치하는 로우가 없으면 null 삽입
joinType = 'outer'

# 8.5 왼쪽 외부 조인
# 왼쪽 df의 모든 로우 + 오른쪽 df의 왼쪽과 일치하는 로우
joinType = 'left_outer'

# 8.6 오른쪽 외부 조인
# 오른쪽 df의 모든 로우 + 왼쪽 df의 오른쪽과 일치하는 로우
joinType = 'right_outer'

# 8.7 왼쪽 세미 조인
# 오른쪽 df의 어떤 값도 포함하지 않음
# 값이 존재하는지 확인을 위해서만 사용
# 값이 존재하면 왼쪽 df에 중복 키가 존재해도 결과에 포함 (필터 기능)
joinType = 'left_semi'

# 8.8 왼쪽 안티 조인
# 왼쪽 세미 조인의 반대 개념
# 오른쪽 df는 값이 존재하는지 확인하는 용도로 사용
# 오른쪽 df에서 관련된 키를 찾을 수 없는 로우만 포함 (NOT IN과 유사)
joinType = 'left_anti'

# 8.9 자연 조인
# 조인하려는 컬럼을 암시적으로 추정
# 부정확한 결과를 만들 수 있어 조심해야 함

# 8.10 교차 조인 (카테시안 조인)
# 조건절을 기술하지 않은 내부 조인
# 왼쪽 모든 로우 + 오른쪽 모든 로우
# e.g 1000 로우, 1000 로우 교차 조인 - 1000000 로우 생성
joinType = 'cross'
# 혹은
person.crossJoin(graduateProgram).show()
# 정말 필요한 경우에만 사용해야 함
```

### 8.11 조인 사용 시 문제점

- 8.11.1 복합 데이터 타입의 조인
    - 불리언을 반환하는 모든 표현식은 조인 표현식으로 간주
- 8.11.2 중복 컬럼명 처리
    - DataFrame의 각 컬럼은 카탈리스트 내에 고유 ID가 있지만 직접 참조할 수 없어서 특정 컬럼을 참조하기 어려움.
    - 중복된 컬럼명을 다루기 힘들다는 문제가 있음
    - 문제가 생기는 두 가지 상황
        - 조인에 사용할 키가 동일한 이름을 가지며, 제거되지 않도록 명시된 상황
        - 조인 대상이 아닌 두 키가 동일한 이름을 가진 상황
    - 해결방법
        1. 다른 조인 표현식 사용
            - 불리언 형태의 조인 표현식을 문자열이라 시퀀스 형태로 변경
            - 조인할 때 하나가 자동 제거
        2. 조인 후 컬럼 제거
            - 조인 후에 문제되는 컬럼 제거
        3. 조인 전 컬럼명 변경

### 8.12 스파크의 조인 수행 방식

- 8.12.1 네트워크 통신 전략
    - 스파크는 조인 시 두 가지 클러스터 통신 방식 활용
        - 셔플 조인 : 전체 노드 간 통신 유발
        - 브로드캐스트 조인 : 전체 노드 간 통신 유발 x
    - 큰 테이블과 큰 테이블 조인
        - 큰 테이블과 큰 테이블을 조인하면 셔플 조인이 발생.
        - 셔플 조인은 전체 노드 간 통신이 발생하고, 조인에 사용한 특정 키나 키 집합을 공유.
        - 네트워크가 복잡해지고 많은 자원 사용
        - 전체 조인 프로세스가 진행되는 동안 모든 워커노드 (파티션 없이)가 통신
    - 큰 테이블과 작은 테이블 조인
        - 최적화 가능.
        - 브로드캐스트 조인이 효율적 - 전체 노드가 통신하는 것을 방지
        - 작은 DataFrame을 클러스터의 전체 워커노드에 복제.
        - 처음에 대규모 통신이 발생하지만 이후에 추가 통신이 발생하지 않음.
        - 너무 큰 데이터를 브로드캐스트하면 고비용의 수집 연산이 발생해서 드라이버 노드의 비정상적 종료 가능성.
    - 아주 작은 테이블 사이의 조인
        - 스파크가 조인 방식을 결정하는게 제일 좋지만 필요한 경우 브로드캐스트 강제 지정 가능.
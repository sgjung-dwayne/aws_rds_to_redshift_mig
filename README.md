
*  MySQL → Redshift 스키마 제한 사항 확인
    *  Redshift에서는 DATETIME 타입은 제공하지 않아 → TIMESTAMP 타입으로 변환
    *  날짜 데이터의 기본 값이 ‘0000-00-00’일 경우 예러 발생 → ‘0101-01-01’로 변환

* 모든 테이블에 PK(Auto_increment) 컬럼 기준으로 분할 조회
    * 조건절 없이 SELECT 할 경우(Full Table Scan) 메모리 사용량에 대한 이슈 확인
    *  공통적으로 id(Auto_increment) 컬럼을 PK로 설정되어 있어 1,500,000건씩 조회(Range Scan)
    *  각 테이블마다 MIN, MAX 값 확인 후 MAX(id)까지 1,500,000건씩 SELECT 수행
*  위 조회 결과들을 S3 CSV 파일로 저장 → Redshift COPY 명령어를 활용하여 병렬 적재(Bulk Insert)
    * 테이블 조회 결과를 S3 내에 업체 별/테이블 별로 CSV 파일 적재
    *  S3로 저장할 시 GZIP 옵션을 사용하여 CSV 파일에 대한 압축 수행

* 각 작업 절차에 대해 Python 스크립트 작성 및 수행
    * EC2 서버에서 Python 스크립트 수행(CPU : 8core, Mem : 32G, 이관 대상 건수 : 약 26억건)
    * MySQL → S3 CSV 파일 적재 : 약 53시간 소요
    * S3 → Redshift Bulk Insert : 약 57시간 소요
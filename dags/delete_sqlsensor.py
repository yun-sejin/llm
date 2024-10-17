Airflow에서 poke는 센서가 주기적으로 특정 조건을 확인하는 방식을 의미합니다. 센서는 주어진 간격(poke_interval)마다 조건을 확인하고, 조건이 만족되면 다음 태스크로 넘어갑니다. 조건이 만족되지 않으면, 센서는 다시 대기하고 주기적으로 조건을 확인합니다.

주요 개념
poke_interval: 센서가 조건을 확인하는 주기(초 단위). 예를 들어, poke_interval=300이면 센서는 5분마다 조건을 확인합니다.
timeout: 센서가 조건을 확인하는 최대 시간(초 단위). 예를 들어, timeout=60이면 센서는 1분 동안 조건을 확인하고, 조건이 만족되지 않으면 실패로 간주합니다.
mode: 센서의 동작 방식을 지정합니다. poke 모드는 주기적으로 조건을 확인하는 방식입니다.
예제 코드에서의 poke 사용

@task_sensor(poke_interval=300, timeout=60, mode='poke')  # 5분마다 실행
def check_upload_delete():
    hook = PostgresHook(postgres_conn_id='your_postgres_connection')
    records = hook.get_records("""
        SELECT 1 
        FROM dsllm_job_his 
        WHERE job_end_point = '/delete' 
          AND job_type = 'fastapi' 
          AND after_job_id IS NULL 
          AND success_yn = TRUE
    """)
    return len(records) > 0


설명
poke_interval=300: 센서는 5분마다 조건을 확인합니다.
timeout=60: 센서는 최대 1분 동안 조건을 확인합니다. 1분이 지나도 조건이 만족되지 않으면 센서는 실패로 간주됩니다.
mode='poke': 센서는 주기적으로 조건을 확인하는 방식을 사용합니다.
이 설정을 통해 센서는 5분마다 데이터베이스 쿼리를 실행하여 조건을 확인하고, 조건이 만족되면 다음 태스크로 넘어갑니다. 조건이 만족되지 않으면 센서는 다시 대기하고, 5분 후에 다시 조건을 확인합니다. 최대 1분 동안 조건이 만족되지 않으면 센서는 실패로 간주됩니다.


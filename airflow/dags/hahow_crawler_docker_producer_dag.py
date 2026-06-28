"""
Hahow 爬蟲 DAG
使用 Docker 容器執行 Hahow 平台的數據爬取任務
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash_operator import BashOperator


# 預設參數
default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,  # 失敗時重試 2 次
    'retry_delay': timedelta(minutes=1),  # 重試間隔 5 分鐘
    'execution_timeout': timedelta(hours=1),  # 執行超時時間 1 小時
}



# 建立 DAG
with DAG(
    dag_id='hahow_crawler_docker_producer_dag',
    default_args=default_args,
    description='Hahow 平台數據爬取 DAG - 爬取課程和文章數據',
    schedule_interval=None,  # 不定期執行
    catchup=False,  # 不執行歷史任務
    max_active_runs=1,  # 同時只允許一個 DAG 實例運行
    tags=['hahow', 'crawler', 'etl', 'docker'],
) as dag:

    # 開始任務
    start_task = BashOperator(
        task_id='start_crawler',
        bash_command='echo "開始執行 Hahow 爬蟲任務..."',
    )

    # Docker 配置
    DOCKER_IMAGE = "enzochang/data_ingestion:latest"
    DOCKER_COMMAND = "uv run --env-file .env python data_ingestion/producer_crawler_hahow_all.py"
    DOCKER_NETWORK = "my_network"

    # Docker 爬取任務
    docker_crawler_task = DockerOperator(
        task_id='docker_hahow_crawler',
        image=DOCKER_IMAGE,
        command=DOCKER_COMMAND,
        network_mode=DOCKER_NETWORK,
        environment={
            'TZ': 'Asia/Taipei'
        },
        auto_remove=True,  # 執行完成後自動刪除容器
    )

    # 結束任務
    end_task = BashOperator(
        task_id='end_crawler',
        bash_command='echo "Hahow 爬蟲任務發送完成！"',
        trigger_rule='all_success',  # 只有當所有前置任務成功時才執行
    )

    # 設定任務依賴關係
    # 開始 -> Docker 爬取任務 -> 結束
    start_task >> docker_crawler_task >> end_task
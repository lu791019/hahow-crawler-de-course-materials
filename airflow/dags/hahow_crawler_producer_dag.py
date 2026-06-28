"""
Hahow 爬蟲 DAG
爬取 Hahow 平台的課程和文章數據，並上傳至 MySQL 資料庫
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# 導入爬蟲任務
from data_ingestion.tasks_crawler_hahow import crawler_hahow_article, crawler_hahow_course

# 定義要爬取的分類
CATEGORIES = [
    "programming", "marketing", "language", "design", 
    "lifestyle", "music", "art", "photography", 'humanities',
    "finance-and-investment", "career-skills", "cooking",
]

# 包裝函數，避免序列化問題
def trigger_course_crawler(category):
    """觸發課程爬蟲任務"""
    crawler_hahow_course.delay(category)

def trigger_article_crawler(category):
    """觸發文章爬蟲任務"""
    crawler_hahow_article.delay(category)

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
    dag_id='hahow_crawler_producer_dag',
    default_args=default_args,
    description='Hahow 平台數據爬取 DAG - 爬取課程和文章數據',
    schedule_interval=None,  # 不定期執行
    catchup=False,  # 不執行歷史任務
    max_active_runs=1,  # 同時只允許一個 DAG 實例運行
    tags=['hahow', 'crawler', 'etl'],
) as dag:

    # 開始任務
    start_task = BashOperator(
        task_id='start_crawler',
        bash_command='echo "開始執行 Hahow 爬蟲任務..."',
    )

    # 課程分流 dummy task
    course_branch = DummyOperator(
        task_id='course_branch',
    )

    # 文章分流 dummy task  
    article_branch = DummyOperator(
        task_id='article_branch',
    )

    # 課程爬取任務 - 為每個分類創建單獨的任務
    course_tasks = []
    for category in CATEGORIES:
        task = PythonOperator(
            task_id=f'crawl_course_{category}',
            python_callable=trigger_course_crawler,
            op_args=[category],
        )
        course_tasks.append(task)

    # 文章爬取任務 - 為每個分類創建單獨的任務
    article_tasks = []
    for category in CATEGORIES:
        task = PythonOperator(
            task_id=f'crawl_article_{category}',
            python_callable=trigger_article_crawler,
            op_args=[category],
        )
        article_tasks.append(task)

    # 結束任務
    end_task = BashOperator(
        task_id='end_crawler',
        bash_command='echo "Hahow 爬蟲任務發送完成！"',
        trigger_rule='all_success',  # 只有當所有前置任務成功時才執行
    )

    # 設定任務依賴關係
    # 開始 -> 環境驗證 -> 兩個分流 -> 各自的爬取任務 -> 結束
    start_task >> course_branch >> course_tasks >> end_task
    start_task >> article_branch >> article_tasks >> end_task
 Data Pipeline Concepts 

	• Pipeline Design
	• DAG (Directed Acyclic Graph)
	• Scheduling Jobs
	• Dependency Management
	• Retry Mechanisms
	• Logging & Monitoring
	• Error Handling
	• Backfilling Data
	1. Pipeline Design


	1️⃣ What is a Data Pipeline?
	👉 Simple definition:
	
	A Data Pipeline is a system that moves data from source → process → destination
	
	🧠 Real World Example
	Think of a company like Amazon
	When you place an order:
	
	Order placed → stored in DB → processed → shown in dashboard
	This flow = Data Pipeline
	
	
	🔄 Pipeline Flow (Real Project)
	
	Source → Staging → Transform → Warehouse → Dashboard
	
	Tools Used in Real Pipelines
		• Processing → Apache Spark
		• Scheduling → Apache Airflow
		• Storage → Amazon S3
		• Warehouse → Snowflake
	
	Explain:
	
	Design a pipeline for order processing system
	Expected answer:
	
	Source → Staging → Dedup → Transform → Fact Table → Dashboard
	
	
	Small Practice (VERY IMPORTANT)
	Try to answer:
	
	If duplicate orders come from source,
	where will you handle it in pipeline?
	
	Duplicates should be handled in the staging layer (or transformation layer)
	before loading into fact tables, using ROW_NUMBER or dedup logic,
	to ensure clean and reliable data in the warehouse.
	
	
	

• DAG (Directed Acyclic Graph)
1️⃣ What is DAG?
👉 DAG = Directed Acyclic Graph
Simple meaning:

A workflow where tasks run in order without loops
🔄 Example Pipeline as DAG

Task 1 → Task 2 → Task 3 → Task 4
Example:

Extract Data → Clean Data → Transform → Load to Warehouse

📊 Real DAG Example (Order Pipeline)

task_1: extract_orders
        ↓
task_2: remove_duplicates
        ↓
task_3: transform_data
        ↓
task_4: load_fact_table

⚠️ Why "Acyclic"?

Task A → Task B → Task C
❌ Task C → Task A (NOT allowed)
No loops allowed.

🧠 DAG in Real Life (Think Like This)
Like cooking:

Cut vegetables → Cook → Serve
You cannot:

Serve → Cook again ❌

Real Interview Question
👉 What is DAG?
Expected answer:

A DAG is a workflow of tasks with defined dependencies
that executes in a specific order without cycles.


If one task fails in DAG,
what should happen next?
Answer: 
If a task fails in a DAG,
only that task is marked as FAILED.

Downstream tasks will NOT run,
but successful tasks will NOT re-run.

The failed task can be retried or restarted manually.


Scheduling Jobs
1️⃣ What is Scheduling?
👉 Simple:

Scheduling = Running your pipeline automatically at specific time intervals

🧠 Real World Example
Company like Amazon needs:

Daily sales report at 8 AM
👉 So pipeline runs automatically every day.

⏰ Types of Scheduling

Hourly
Daily
Weekly
Real-time (near real-time)

🔄 Real Pipeline with Schedule
Pipeline:

Extract → Dedup → Transform → Load



Failure + Scheduling
If pipeline fails at 2 AM:

Data not loaded ❌
Dashboard incorrect ❌

🔁 Solution
Retry OR rerun manually.



Real Concept: Backfill (Important)
👉 Suppose:

Pipeline failed for 3 days
Then:

We run pipeline for missed dates
Example:

SELECT *
FROM src_orders
WHERE updated_at BETWEEN '2026-03-01' AND '2026-03-03';
👉 This is called Backfilling

🧠 Real Scenario (Very Important)

March 1 ❌ failed
March 2 ❌ failed
March 3 ✅ success
👉 Engineer runs:

Backfill for March 1 & 2

🧠 Interview Answer
👉 What is Scheduling?

Scheduling is the process of running data pipelines automatically
at defined intervals like hourly, daily, or weekly.

👉 What is Backfilling?

Backfilling is reprocessing data for past dates when pipeline execution failed or was missed.


👉If a daily pipeline fails today,
how will you ensure data is not lost?

If a daily pipeline fails, I will rerun the failed pipeline
and perform backfilling for the missed date using incremental logic
to ensure no data loss.


Incremental logic example

SELECT *
FROM src_orders
WHERE updated_at = CURRENT_DATE;

• Retry Mechanisms
1️⃣ What is Retry Mechanism?
👉 Simple:

Retry = Automatically re-running a failed task after some time

🧠 Why Retry is Needed?
In real systems (like Amazon):
Failures are common:

DB connection failed
API timeout
Network issue
Temporary system error
👉 These are temporary issues, not permanent.

🎯 Goal of Retry

Fix temporary failures automatically without manual work

⏱️ When to Use Retry?
Use retry when failure is:

Temporary ✅
Intermittent ✅
External system issue ✅

❌ When NOT to Use Retry

Wrong SQL query ❌
Logic error ❌
Missing column ❌
👉 Retry won’t fix these.

🔧 Practical Example (Airflow Style)
Using
Apache Airflow

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_data():
    print("Extracting data...")

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'order_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval='@daily'
)

task1 = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_data,
    dag=dag
)

🔁 What this means

If task fails → retry 3 times
Wait 5 minutes between retries

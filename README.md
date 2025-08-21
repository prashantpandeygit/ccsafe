## **CCSAFE**
This project demonstrates a realtime data engineering pipeline for processing, validating, and visualizing credit card transactions using Google Cloud Platform (GCP).

We automate ingestion of transaction files after conversion, process them with PySpark, archive raw data, safeguard transactions, and create dashboards in Looker Studio for analytics and monitoring.
## **Tech Stack**

**Cloud Platform:** Google Cloud Platform (GCP)

**Storage:** Google Cloud Storage (GCS)

**Processing:** Dataproc (PySpark) 

**Data Warehouse:** BigQuery 

**Orchestration:** Apache Airflow (Cloud Composer) 

**Visualization:** Looker Studio 

**Version Control & CI/CD:** GitHub + GitHub Actions 


## **Pipeline Flow**

**File Ingestion:** Transaction files are uploaded to GCS.

**Airflow DAG:** Detects new files and triggers pipeline.

**Dataproc Batch Job:** Executes PySpark job to process and enrich transactions.

**Archival:** Raw files are moved from transactions to archive after processing.

**Visualization:** Processed data is visualized in Looker Studio dashboards for analytics, fraud detection, and reporting.
## **Run Locally**

**Clone the Repository**

```bash
git clone https://github.com/prashantpandeygit/ccsafe.git
cd ccsafe
```

**Setup Virtual Environment**

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Configure Environment and GCP Credentials**

```bash
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1
GCS_BUCKET=your-storage
DATAPROC_SERVICE_ACCOUNT=your-sa@ccsafe.iam.gserviceaccount.com
```

**Deploy to Airflow (Cloud Composer)**

```bash
gcloud composer environments storage dags import \
  --environment airflow-your-project \
  --location us-central1 \
  --source airflow.py
```

**Upload Spark Job**

```bash
gsutil cp sparkjob.py gs://your-storage/sparkjob/
```


## **Running Tests**

To run tests, run the following command

```bash
  pytest -v
```

## **CI/CD Workflow**

On push to dev branch → Run tests.

If tests pass → Push to dev branch -> Create a Pull Request to main.

On merge to main → DAG and Spark job to GCP deployment will be done.
## **Dashboard**

```bash
https://lookerstudio.google.com/u/0/reporting/7b01301c-2e01-4917-804c-aabf7a008894/page/UrPVF
```
## **Dashboard Overview**
![looker-d.png](https://i.postimg.cc/W4MrWX4H/looker-d.png)


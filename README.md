* Airflow:
  - airflow db init
  - airflow webserver
  - airflow scheduler
  - airflow.cfg is located here: /home/feuer/airflow/airflow.cfg
    <br> All changes are already present there.
  - To view: <br> http://localhost:6007/
* MlFlow:
  - mlflow server --backend-store-uri='sqlite:///./02_training_pipeline/notebooks/lead_scoring_model_experimentation.db' --default-artifact-root="./mlruns" --port=6006 --host=0.0.0.0
  - To view: http://0.0.0.0:6006/
  - For training pipeline:
    mlflow server --backend-store-uri='sqlite:///./02_training_pipeline/notebooks/Lead_scoring_mlflow_production.db' --default-artifact-root="./mlruns" --port=6008 --host=0.0.0.0
* JupyterLab:
  - Open Assignments dir and then jupyter-lab
!pip install MLflow
import mlflow
import mlflow.sklearn
from google.cloud import bigquery
from datetime import datetime
import pandas as pd

# Setting up MLflow
mlflow.set_tracking_uri("http://34.141.32.30:5000")
mlflow.set_experiment("MPD_Analysis_Experiment")

# Initializing BigQuery client
client = bigquery.Client()


# 1: Data Preparation
def prepare_data(countries):
    countries_str = ', '.join([f"'{country}'" for country in countries])
    query = f"""
    CREATE OR REPLACE TABLE `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_SALES_FORECAST` AS
SELECT A.CustomerId,A.Country, B.OrderNumber,B.Date,C.ProductId,C.SalesQuantity, D.ProdDescr
FROM  `vlba-2024-mpd-group-5.MPD_DATA_5.MPD-CustomerAttr` AS A
JOIN `vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Orders` AS B ON A.CustomerID = B.CustomerID
JOIN `vlba-2024-mpd-group-5.MPD_DATA_5.MPD-OrderItems` AS C ON B.OrderNumber = C.OrderNumber
JOIN  `vlba-2024-mpd-group-5.MPD_DATA_5.MPD_ProductText` AS D ON C.ProductId= D.ProductId
WHERE A.Country IN({countries_str});
    """

    client.query(query).result()
    query_results = client.query(f"SELECT * FROM `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_SALES_FORECAST`").result()
    df = query_results.to_dataframe()
    df.to_csv('/tmp/sales_table.csv', index=False)
    mlflow.log_artifact('/tmp/sales_table.csv', artifact_path="data")

    q1 = f"""
    CREATE OR REPLACE TABLE `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_MATERIAL_FORECAST` AS
  SELECT A.Date,A.ProductId,A.ProdDescr,B.MatId,
  (B.Lot_size * CAST(B.Amount_Required AS FLOAT64)) AS Total_materials_Required
  FROM `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_SALES_FORECAST` AS A
  JOIN `vlba-2024-mpd-group-5.MPD_DATA_5.MPD-ProductMat` AS B
  ON A.ProductId = B.ProductId;

    """

    client.query(q1).result()
    query_results = client.query(f"SELECT * FROM `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_MATERIAL_FORECAST`").result()
    df = query_results.to_dataframe()
    df.to_csv('/tmp/materials_table.csv', index=False)
    mlflow.log_artifact('/tmp/materials_table.csv', artifact_path="data")

# 2: Model Training
def train_model_sales(countries, start_year, end_year):
    countries_str = ', '.join(f"'{country}'" for country in countries)
    bq_query = f"""
    CREATE OR REPLACE MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Product_Sales_Forecast`
OPTIONS(
  model_type='ARIMA_PLUS',
  auto_arima=True,
  time_series_data_col='total_sales',
  time_series_timestamp_col='Year',
  time_series_id_col='ProdDescr'
) AS
WITH sales_data AS (
  SELECT
    DATE_TRUNC(Date, YEAR) AS Year,
    SUM(SalesQuantity) AS total_sales,
    ProdDescr
  FROM
    `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_SALES_FORECAST`
  GROUP BY
    Year, ProdDescr
  HAVING
    EXTRACT(YEAR FROM Year) BETWEEN {start_year} AND {end_year}
)
SELECT
  Year,
  total_sales,
  ProdDescr
FROM
  sales_data;

    """

    query_job = client.query(bq_query)
    query_job.result()


def train_model_mat(countries, start_year, end_year):

    countries_str = ', '.join(f"'{country}'" for country in countries)

    bq_query1 = f"""CREATE OR REPLACE MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.Material_Forecast`
OPTIONS(
  model_type='ARIMA_PLUS',
  auto_arima=True,
  time_series_data_col='total_materials_required',
  time_series_timestamp_col='Year',
  time_series_id_col='MatId'
) AS
WITH material_data AS (
  SELECT
    DATE_TRUNC(Date, YEAR) AS Year,
    SUM(Total_materials_Required) AS total_materials_required,
    MatId
  FROM
    `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_MATERIAL_FORECAST`
  GROUP BY
    Year, MatId
  HAVING
    EXTRACT(YEAR FROM Year) BETWEEN {start_year} AND {end_year}
)
SELECT
  Year,
  total_materials_required,
  MatId
FROM
  material_data;"""

    qj = client.query(bq_query1)
    qj.result()


#3: Model Evaluation
def evaluate_model_sales():
    eval_query1 = """
    Select
     *
    from
      ML.EVALUATE(MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Product_Sales_Forecast`);
    """
    eval_job = client.query(eval_query1)
    eval_result = eval_job.result()
    eval_df = eval_result.to_dataframe()
    eval_df.to_csv('/tmp/sales_training_evaluation.csv', index=False)
    mlflow.log_artifact('/tmp/sales_training_evaluation.csv', artifact_path="training_evaluation")
    return eval_df


def evaluate_model_mat():
    eval_query = """
    Select
     *
    from
      ML.EVALUATE(MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.Material_Forecast`);
    """
    eval_job = client.query(eval_query)
    eval_result = eval_job.result()
    eval_df = eval_result.to_dataframe()
    eval_df.to_csv('/tmp/material_training_evaluation.csv', index=False)
    mlflow.log_artifact('/tmp/material_training_evaluation.csv', artifact_path="training_evaluation")
    return eval_df




# 4: Sales Forecasting for period of time
def forecast_sales_for_period(forecast_period_in_years):

    sales_query = f"""
    select * from ML.FORECAST(MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Product_Sales_Forecast`, STRUCT({forecast_period_in_years} AS horizon, 0.99 AS confidence_level));
    """
    forecast_job = client.query(sales_query)
    forecast_result = forecast_job.result()
    sales_df = forecast_result.to_dataframe()
    sales_df.to_csv(f"/tmp/sales_forecast_for_period_{forecast_period_in_years}_years.csv", index=False)
    mlflow.log_artifact(f"/tmp/sales_forecast_for_period_{forecast_period_in_years}_years.csv", artifact_path="forecast")
    return sales_df

def forecast_mat_for_period(forecast_period_in_years):
    mat_query = f"""WITH forecast_data AS (
  SELECT
    *
  FROM
    ML.FORECAST(MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.Material_Forecast`,
                STRUCT(5 AS horizon, 0.99 AS confidence_level))
)
SELECT
  m.ProdDescr,
  f.MatId,
  f.forecast_timestamp AS Year,
  f.forecast_value AS total_materials_required,
  f.standard_error,
  f.confidence_level,
  f.prediction_interval_lower_bound,
  f.prediction_interval_upper_bound,
  f.confidence_interval_lower_bound,
  f.confidence_interval_upper_bound
FROM
  forecast_data AS f
JOIN
  (SELECT DISTINCT MatId, ProdDescr
   FROM `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_MATERIAL_FORECAST`) AS m
ON
  f.MatId = m.MatId
ORDER BY
  m.ProdDescr, f.MatId, f.forecast_timestamp;"""

    forecast_job = client.query(mat_query)
    forecast_result = forecast_job.result()
    materials_df = forecast_result.to_dataframe()
    materials_df.to_csv(f"/tmp/materials_forecast_for_period_{forecast_period_in_years}_years.csv", index=False)
    mlflow.log_artifact(f"/tmp/materials_forecast_for_period_{forecast_period_in_years}_years.csv", artifact_path="forecast")
    return materials_df


# 5: Sales Forecasting for particular year
def forecast_sales_for_year(end_year, forecast_year):
    horizon_years = (forecast_year - end_year) + 1
    forecast_query = f"""
    select * from ML.FORECAST(MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Product_Sales_Forecast`, STRUCT({horizon_years} AS horizon, 0.99 AS confidence_level))
    where Extract(YEAR from forecast_timestamp) = {forecast_year};
    """
    forecast_job = client.query(forecast_query)
    forecast_result = forecast_job.result()
    forecast_df = forecast_result.to_dataframe()
    forecast_df.to_csv(f"/tmp/sales_forecast_for_year_{forecast_year}.csv", index=False)
    mlflow.log_artifact(f"/tmp/sales_forecast_for_year_{forecast_year}.csv", artifact_path="forecast")
    return forecast_df


def forecast_mat_for_year(end_year, forecast_year):
      horizon_years = (forecast_year - end_year) + 1
      mat_query = f"""WITH forecast_data AS (
      SELECT
        *
      FROM
        ML.FORECAST(MODEL `vlba-2024-mpd-group-5.MPD_DATA_5.Material_Forecast`,
                    STRUCT(5 AS horizon, 0.99 AS confidence_level))
                    where Extract(YEAR from forecast_timestamp) = {forecast_year}
    )
    SELECT
      m.ProdDescr,
      f.MatId,
      f.forecast_timestamp AS Year,
      f.forecast_value AS total_materials_required,
      f.standard_error,
      f.confidence_level,
      f.prediction_interval_lower_bound,
      f.prediction_interval_upper_bound,
      f.confidence_interval_lower_bound,
      f.confidence_interval_upper_bound
    FROM
      forecast_data AS f
    JOIN
      (SELECT DISTINCT MatId, ProdDescr
      FROM `vlba-2024-mpd-group-5.MPD_DATA_5.TASK2_MPD_DATA_FOR_MATERIAL_FORECAST`) AS m
    ON
      f.MatId = m.MatId
    ORDER BY
      m.ProdDescr, f.MatId, f.forecast_timestamp;"""
      forecast_job = client.query(mat_query)
      forecast_result = forecast_job.result()
      forecast_df = forecast_result.to_dataframe()
      forecast_df.to_csv(f"/tmp/materials_forecast_for_year_{forecast_year}.csv", index=False)
      mlflow.log_artifact(f"/tmp/materials_forecast_for_year_{forecast_year}.csv", artifact_path="forecast")
      return forecast_df


# 6: Log Parameters and Metrics
def log_params_and_metrics(countries, syr, eyr):
    mlflow.log_param("model_type", "ARIMA_PLUS")
    mlflow.log_param("auto_arima", "True")
    mlflow.log_param("Time Series", "Year")
    mlflow.log_param("Countries", countries)
    mlflow.log_param("Training Start Year", syr)
    mlflow.log_param("Training End Year", eyr )


def log_sales_param_and_metrics(eval_df):
    numeric_columns = ['non_seasonal_p', 'non_seasonal_d', 'non_seasonal_q', 'log_likelihood', 'AIC', 'variance']
    for col in numeric_columns:
        if col in eval_df.columns:
            metric_key = f"sales_mean_{col}"
            value = eval_df[col].mean()
            mlflow.log_metric(metric_key, value)

def log_mat_param_and_metrics(eval_df):
    numeric_columns = ['non_seasonal_p', 'non_seasonal_d', 'non_seasonal_q', 'log_likelihood', 'AIC', 'variance']
    for col in numeric_columns:
        if col in eval_df.columns:
            metric_key = f"materials_mean_{col}"
            value = eval_df[col].mean()
            mlflow.log_metric(metric_key, value)

# 7: Register the Model
def register_model():
    model_uri = "bigquery://vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Product_Sales_Forecast"
    mlflow.register_model(model_uri, "MPD_Sales_Model")

    model_uri = "bigquery://vlba-2024-mpd-group-5.MPD_DATA_5.MPD_Material_Forecast"
    mlflow.register_model(model_uri, "MPD_Material_Model")


if __name__ == "__main__":
    with mlflow.start_run(run_name="Sales_Forecasting_Workflow") as run:

        countries = input("Enter countries (comma-separated): ").strip().split(',')
        training_start_year = int(input("Enter training start year: ").strip())
        training_end_year = int(input("Enter training end year: ").strip())
        # Step 1: Data Preparation
        prepare_data(countries)
        # Step 2: Model Training
        train_model_sales(countries, training_start_year, training_end_year)
        # Step 3: Model Evaluation
        eval_sales_df = evaluate_model_sales()
        # Step 2: Model Training
        train_model_mat(countries, training_start_year, training_end_year)
        # Step 3: Model Evaluation
        eval_mat_df = evaluate_model_mat()
        period = int(input("Enter forecast period (in years): ").strip())
        year = int(input("Enter a specific year for forecast: ").strip())
        #Step 4: Forecast
        sales_period_forecast_df = forecast_sales_for_period(period)
        mat_period_forecast_df = forecast_mat_for_period(period)
        sales_year_forecast_df = forecast_sales_for_year(training_end_year, year)
        mat_year_forecast_df = forecast_mat_for_year(training_end_year, year)
        # Step 5: Log Parameters and Metrics
        log_params_and_metrics(countries, training_start_year, training_end_year)
        log_sales_param_and_metrics(eval_sales_df)
        log_mat_param_and_metrics(eval_mat_df)

        # Step 6: Register the Model
        register_model()
        mlflow.set_tag("Description", "Forecasting workflow using ARIMA_PLUS model")
        mlflow.sklearn.log_model("ARIMA_PLUS_Model", "models")

        mlflow.end_run()




## RedFin Data Analytics Pipeline

### Overview & Architecture 

![RedFin Chart](https://github.com/abhishekshah25/redfin-data-pipeline/assets/147745895/01e5a82d-4a5e-432f-bd21-147151a9447f)

The RedFin Data Pipeline is designed to streamline & automate the process of extracting, transforming and visualizing real estate data. By leveraging tools and services like Apache Airflow, Amazon EC2, AWS S3, Amazon EMR, Snowflake and Power BI, this pipeline efficiently handles over 200,000 records monthly ensuring scalable & reliable data processing & insightful market analysis.

![DAG_RedFin](https://github.com/abhishekshah25/redfin-data-pipeline/assets/147745895/0682ffcc-94b1-41e1-81ac-d61021ed200c)

The pipeline leverages the following services:

1. Amazon EC2: Hosts Apache Airflow for orchestrating the ETL processes.
2. AWS S3: Holds the data which is to be later used by Amazon EMR for data transformation.
3. Amazon EMR: Handles data transformation tasks & loads the transformed data back to S3 to be further consumed by Snowflake.
4. Snowflake: Leverages Snowpipe to auto-populate the transformed data from S3 to Snowflake Databases.
5. Power BI: Visualizes the Snowflake database data for insightful reporting.


### Data Flow

#### Extraction:

-> Use the RedFin API to extract real estate data


![S3_RedFin](https://github.com/abhishekshah25/redfin-data-pipeline/assets/147745895/1992d6f9-2a3d-40c8-9b23-24204345b226)

#### Transformation:

-> Apache Airflow on EC2 orchestrates the ETL processes.

-> Amazon EMR performs data transformation tasks to ensure data is cleaned & transformed for further consumption.


![EC2_RedFin](https://github.com/abhishekshah25/redfin-data-pipeline/assets/147745895/49b2957f-202e-41b1-ad76-1989cbfcffe0)


#### Loading:

-> Transformed data is loaded into Snowflake Database via Snowpipe for efficient storage and analysis.

![Snowpipe](https://github.com/abhishekshah25/redfin-data-pipeline/assets/147745895/12170eec-18a1-4a74-8a80-912f177edac0)


#### Visualization:

-> Power BI helps in creating interactive dashboards to visualize real estate market trends.


![BI_Dashboard](https://github.com/abhishekshah25/redfin-data-pipeline/assets/147745895/55fe2b65-7b26-4dd4-9bf2-c51a55b103f8)


### Features

1. Scalable: The pipeline processes over 100,000 records monthly.
2. Efficient: Ensures efficient data handling via automated ETL processes.
3. Insightful: Provides detailed & interactive visualizations for real estate market analysis.

### Getting Started

To get started with the data pipeline, follow the steps mentioned in the Procedure file. Feel free to make modifications in the data flow structure while creating your own pipeline.

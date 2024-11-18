# **Earthquake Data Processing Pipeline**

This project automates the ingestion, transformation, and analysis of earthquake data from the USGS Earthquake API. The pipeline processes raw GeoJSON data, flattens nested structures, converts epoch timestamps to readable formats, and extracts geographical information.

## **Key Features**
- **Data Ingestion:** Fetch data from the USGS Earthquake API.
- **Data Transformation:** Perform key transformations using PySpark or Apache Beam (Cloud Dataflow).
- **Cloud Storage:** Store processed data in Google Cloud Storage (Parquet format).
- **BigQuery Integration:** Load structured data into BigQuery for advanced querying and analysis.
- **Analysis:** Generate insights on earthquake patterns, frequency, and magnitude distribution.

## **Tech Stack**
- **Frameworks:** PySpark, Apache Beam (Dataflow).
- **Cloud Platform:** Google Cloud Platform (GCS, BigQuery).
- **Programming Language:** Python

## **Pipeline Flow**
1. **Source:** USGS Earthquake API  
2. **Processing:** PySpark/Apache Beam for data transformation  
3. **Storage:** Google Cloud Storage (Silver Layer)  
4. **Analysis:** BigQuery  

## **How to Run**
1. Clone the repository and install dependencies.  
2. Execute the pipeline using either PySpark or Apache Beam.  
3. Analyze the data directly in BigQuery.  

This project demonstrates a robust and scalable approach to processing and analyzing real-world seismic data using modern cloud-native tools.

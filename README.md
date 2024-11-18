<!DOCTYPE html>
<html>
<head>
    <title>Earthquake Data Processing Pipeline</title>
</head>
<body>
    <h1>Earthquake Data Processing Pipeline</h1>
    <p>
        This project automates the ingestion, transformation, and analysis of earthquake data from the USGS Earthquake API. 
        The pipeline processes raw GeoJSON data, flattens nested structures, converts epoch timestamps to readable formats, 
        and extracts geographical information.
    </p>

    <h2>Key Features</h2>
    <ul>
        <li><strong>Data Ingestion:</strong> Fetch data from the USGS Earthquake API.</li>
        <li><strong>Data Transformation:</strong> Perform key transformations using PySpark or Apache Beam (Cloud Dataflow).</li>
        <li><strong>Cloud Storage:</strong> Store processed data in Google Cloud Storage (Parquet format).</li>
        <li><strong>BigQuery Integration:</strong> Load structured data into BigQuery for advanced querying and analysis.</li>
        <li><strong>Analysis:</strong> Generate insights on earthquake patterns, frequency, and magnitude distribution.</li>
    </ul>

    <h2>Tech Stack</h2>
    <ul>
        <li><strong>Frameworks:</strong> PySpark, Apache Beam (Dataflow)</li>
        <li><strong>Cloud Platform:</strong> Google Cloud Platform (GCS, BigQuery)</li>
        <li><strong>Programming Language:</strong> Python</li>
    </ul>

    <h2>Pipeline Flow</h2>
    <ol>
        <li><strong>Source:</strong> USGS Earthquake API</li>
        <li><strong>Processing:</strong> PySpark/Apache Beam for data transformation</li>
        <li><strong>Storage:</strong> Google Cloud Storage (Silver Layer)</li>
        <li><strong>Analysis:</strong> BigQuery</li>
    </ol>

    <h2>How to Run</h2>
    <ol>
        <li>Clone the repository and install dependencies.</li>
        <li>Execute the pipeline using either PySpark or Apache Beam.</li>
        <li>Analyze the data directly in BigQuery.</li>
    </ol>

    <p>
        This project demonstrates a robust and scalable approach to processing and analyzing real-world seismic data using 
        modern cloud-native tools.
    </p>
</body>
</html>

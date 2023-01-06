## Scope

The goal of this project is to build a pipeline that requests data from Spotify. The pipeline should be able to handle data requests regarding artists contents, store the requested data in a secure database where queries can be run against, to answer the questions below.
* In which year/decade did they release the most albums/songs?
* What is the shortest/longest album? (in minutes).
* What is the loudest album?

To achieve this goal, the project will include the following steps:

1. Setting up a secure server and database (data lake).
2. Creating an ETL (Extract, Transform, Load) pipeline to gather data from the Spotify API.
3. Extracting the data from the Spotify API into the data lake.
4. Transforming the data into a format that can be easily queried, such as a tabular format.
5. Creating queries to answer the questions. For example, to answer the first question (In which year/decade did they release the most albums/
songs), a query can be created to group the albums/songs by year/decade and count the number of albums/songs released in each.
6. Testing and debugging the pipeline to ensure it is functioning properly.
7. Documenting the project and creating user guides to ensure smooth user experience.

## Requirements

* The pipeline shall support scalability as the number of albums varies from one artist to another, therefore the system shall be able to handle a growing amount of data. 
* The pipeline shall transform the data gathered to fix problems within the data like empty data or wrong format.  This can potentially include data type casting, joins, and aggregations.
* The data shall be loaded into a data lake storage system to allow further processing and analysis of the data.
* The pipeline shall be able to extract and store the data in a structured format.
* The pipeline shall be able to monitor and alert users to any anomalies in the data.
* The pipeline shall have an automated testing system to ensure the data is correct and within the expected parameters.

## Non-Functional Requirements

* The pipeline shall be secure and have proper access control and authentication mechanisms.
* The pipeline shall be reliable, robust and fault tolerant.
* The pipeline shall have a low latency and be able to process data in a timely manner.
* The pipeline shall have a scalable architecture to support large workloads.
* The pipeline shall have a flexible architecture that can be easily adapted to changing requirements.

## Initial Design

1. Client sends API request to Spotify API.
2. API request is processed by the Spotify API server.
3. API server queries the Spotify database for the requested information.
4. Database returns the requested information to the API server.
5. API server returns the data to the client.
6. The fetched data is processed to filter out only the needed data from the response.
7. The processed data is loaded into a database.

## Background
### Context
Spotify is a popular music streaming service that provides access to millions of songs and artist information. The Spotify API provides an easy way to access and query a vast amount of that music data, which means accessing detailed information about specific artists and their albums. However, many of the data formats provided by the Spotify API are limited and difficult to access and query. In order to access and query these data formats, a data pipeline can be implemented. A data pipeline is a series of steps that are taken to extract data from a source, transform it into a usable format, and store it in a database. This process allows to access and query data formats that are impossible to get with raw data from Spotify.

### Scope
This project will focus on developing a data acquisition and preprocessing pipeline using the Spotify API to request artist data. It will be implemented using Python, and the data will be stored in a relational database such as SQL Server. Subsequently, queries can be run to answer the questions below.
* What is the shortest/longest album? (in minutes).
* What is the loudest album?

## Requirements
* The pipeline shall support scalability as the number of albums varies from one artist to another, and the system shall be able to handle a growing amount of data.
* The pipeline shall transform the data gathered to fix problems within the data, such as empty data or wrong format. That can potentially include data type casting, joins, and aggregations.
* The data shall be loaded into a data lake storage system to allow further processing and analysis of the data.
* The pipeline shall be able to extract and store the data in a structured format.
* The data pipeline shall be able to monitor and alert users to any anomalies in the data.

## Design
### Data Acquisition: 
1. Obtain an API key from the Spotify Developer website.
2. Use the API key to access the Spotify API and requests the artist's data.
3. Get the JSON out of the response object.

### Data Preprocessing and transformation: 
1. Clean up the data by removing any unnecessary columns or rows.
2. Normalize the data by converting any dates or times into a uniform format.
3. Perform any necessary data transformations, such as aggregations or summaries.
4. Validate the data by checking for any inconsistencies or errors.

### Loading: 
5. Load the data into the SQL Server database on Azure.
6. Create and run queries to answer the questions. For instance, to answer the first question (What is the shortest/longest album?), a query can be created to group the songs by album and sum the track length for each album.
7. Test and debug the pipeline to ensure it is functioning correctly.

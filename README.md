## Background
### Context
Spotify is a popular music streaming service that provides access to millions of songs and artist information. The Spotify API provides an easy way to access and query a vast amount of that music data, which means accessing detailed information about specific artists and their albums. However, many of the data formats provided by the Spotify API are limited and difficult to access and query. In order to access and query these data formats, a data pipeline can be implemented. A data pipeline is a series of steps that are taken to extract data from a source, transform it into a usable format, and store it in a database. This process allows to access and query data formats that are impossible to get with raw data from Spotify.

### Scope
This project will focus on developing a data acquisition and preprocessing pipeline using the Spotify API to request artist data. It will be implemented using Python, and the data will be stored in a relational database such as SQL Server. Subsequently, queries can be run to answer the questions below.
* What is the shortest/longest album? (in minutes).
* What is the loudest album?

## Requirements

### Functional
* The pipeline should retrieve and access an OAuth token from Spotify's API to authorize data extraction.
* The pipeline should search for an artist based on the user's input and retrieve information such as name, id, number of followers and genres
* The pipeline should display the results of the search to the user and allow them to select an artist by index number.
* The pipeline shall support scalability as the number of albums varies from one artist to another, and the system shall be able to handle a growing amount of data.
* The pipeline shall transform the data gathered to fix problems within the data, such as empty data or wrong format. That can potentially include data type casting, joins, and aggregations.
* The pipeline should clean and validate the extracted data, checking for unique primary keys and removing null values.
* The pipeline should store the extracted and cleaned data in a database.

### Non-Functional
* The pipeline should use the requests library for API calls, pandas for data manipulation and storage, and SQLAlchemy for database connectivity and management.
* The pipeline should be able to handle large amounts of data with high performance and efficiency.
* The pipeline should provide clear and informative error messages in case of any failures or issues.

## Design
### Data Acquisition: 
1. Obtain an API key from the Spotify Developer website.
2. Use the API key to access the Spotify API and request the artist's data - by user inputting the artist's name
3. The results are returned in JSON format.
4. Transform the data by parsing the JSON response and storing the relevant information in a list of dictionaries

### Data Preprocessing and transformation: 
1. Clean up the data by removing any unnecessary columns or rows.
2. Normalize the data by converting any dates or times into a uniform format.
3. Perform any necessary data transformations, such as aggregations or summaries.
4. Validate the data by checking if the primary key is unique and if there are any null values in the data.

### Loading: 
5. Load the data into the SQL Server database on Azure.
6. Create and run queries to answer the questions. For instance, to answer the first question (What is the shortest/longest album?), a query can be created to group the songs by album and sum the track length for each album.
7. Test and debug the pipeline to ensure it is functioning correctly.


# YouTubeMostPopularVideos
ETL data pipeline using YouTube API, AWS EC2, and AWS RDS, with EDA and Tableau visualizations.

## What I did
This repository holds Python code and a bash shell script that run on an Amazon Web Services (AWS) Elastic Compute Cloud (EC2) instance. Data relating to the current top 200 most popular videos on YouTube are extracted using the YouTube API, transformed using Pandas, and loaded into a data warehouse in AWS' Relational Database Service (RDS).

Data Warehouse Diagram:
![Database Diagram](https://github.com/AndrewDettor/YouTubeMostPopularVideos/blob/main/Database%20Diagram.png?raw=true)

Data is then pulled into a local Jupyter Notebook instance using an SSH tunnel and processed further for exploratory data analysis (EDA).

Insights gained from the EDA are used to create a dashboard in Tableau displaying key information about the views on the most popular YouTube videos.

Tableau Dashboard:
![Tableau Dashboard](https://github.com/AndrewDettor/YouTubeMostPopularVideos/blob/main/YouTube%20Views%20Dashboard.png?raw=true)

## Skills I learned
- Requesting and parsing data from multiple API endpoints
- Designing a data warehouse
- Networking cloud-based compute and database services together
- Creating and automating an ETL pipeline using Python and SQL
- Processing and visualizing multiple multi-variate time series data
- Making a Tableau dashboard

## Insights I gained
The EC2 free tier instance kept running out of memory. I couldn't even install some Python packages I wanted. I also wanted to learn and use Apache Airflow on the instance but I had to use a simpler solution and just use CRON jobs.

The RDS instance was in a private subnet and didn't have a free public IPv4 address. I had to use the EC2 instance (which did have a free public IPv4 address) as a proxy via SSH tunnel when accessing the data locally.

Unexpected AWS costs are almost impossible to track down and stop.

I had to transfer data from one ETL script into the next one, so I just wrote and read from a text file. It's definitely not the best solution, but it works.

The initial ETL into an empty a database can look entirely different from an ETL that updates data already in the database.

Trying to analyze many multi-variate time series of different lengths and different start points proved to be quite difficult. I standardized them to the time since they first showed up on the most popular list and analyzed snapshots of time, like the first time point, the first day, and the first week.

Tableau seems easy on the surface but making visualizations that don't look horrible, are understandable, and convey important information was nothing but easy. Also, some basic things (like a correlation heatmap) can only be done with super hacky methods.





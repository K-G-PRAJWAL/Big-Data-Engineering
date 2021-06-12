# Spotify ETL Data Pipeline

In this project, I have built a simple ETL pipeline using the Spotify API which will download the data about the songs that user listen throughout a day and save them to a database.

In the **Extract** phase, I have downloaded the data from the vendor(Spotify). The extracted data will be in the JSON format. Head  over to [Spotify](https://www.youtube.com/redirect?event=video_description&v=dvviIUKwH7o&q=https%3A%2F%2Fdeveloper.spotify.com%2Fconsole%2Fget-recently-played%2F&redir_token=QUFFLUhqbWU2cGVkc2NrS3BvYjZIaWN0Ym1RRVlzR21Kd3xBQ3Jtc0tscnFZY1NwWDJuQTR6SFdLVFVMSDg3Mlo0eU5mMVpsQmtvVTJkZlRvMzd1T1VpU2pWd1dmVTJjU19UaTRubEhYd25neEN1cWVWV2RtSUo3akp2U3RmWlZ1Q0p1a2UwTkFYTkkzUkUtX0RpQU9RY2RTUQ%3D%3D) to create an API token. 

The username and the API token must be entered in an auth.py file in the dags folder. The spotify_etl.py file handles all the Extract, Transform and Load phases. The spotify_dag.py folder is configured to work with airflow. This is done in such a way that each day the list of songs heard yesterday will be loaded into the sqlite database.

---

Commands to keep in mind with respect to airflow:
```
pip install apache-airflow
airflow webserver -p 8080
airflow initdb
airflow scheduler
```

---

Thank You! :relaxed:

---
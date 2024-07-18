@echo off

REM Change to the directory containing the scripts and files
cd /d C:\Users\detto\Documents\YouTubeViewPrediction\ETLs

REM Execute the Python scripts in sequence with a sleep interval
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe video_dim_ETL.py
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe channel_dim_ETL.py
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe categories_dim_ETL.py
timeout /t 1 /nobreak
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe video_fact_ETL.py
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe channel_fact_ETL.py

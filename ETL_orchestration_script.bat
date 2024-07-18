@echo off

REM Change to the directory containing the scripts and files
cd /d C:\Users\detto\Documents\YouTubeViewPrediction

REM Execute the Python scripts in sequence with a sleep interval
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe new_categories_ETL.py
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe new_videos_ETL.py
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe new_channels_ETL.py
timeout /t 2 /nobreak
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe update_videos_ETL.py
C:\Users\detto\AppData\Local\Programs\Python\Python310\python.exe update_channels_ETL.py

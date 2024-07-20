#!/bin/bash -l

# Activate virtual environment
source ~/anaconda3/etc/profile.d/conda.sh
conda activate base

# Change to the directory containing the scripts and files
cd /home/ec2-user/YouTubeViewPrediction/ETLs

# Print current date and time for logging reasons
echo "Starting python ETL scripts at $(date)"

# Execute the Python scripts in sequence
/home/ec2-user/anaconda3/bin/python video_dim_ETL.py
/home/ec2-user/anaconda3/bin/python channel_dim_ETL.py
/home/ec2-user/anaconda3/bin/python video_fact_ETL.py
/home/ec2-user/anaconda3/bin/python channel_fact_ETL.py

# Deactivate the virtual environment
conda deactivate
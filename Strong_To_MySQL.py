#This is a program to save my weightlifting data to MySQL workbench


import pandas as pd
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL, Time, DateTime, Date
from datetime import datetime
import numpy as np
from sqlalchemy.exc import OperationalError
from sqlalchemy import inspect


# Find downloaded data from the Strong App and read it into a DF using pandas

strong_path = '/Users/aidanolander/Documents/AidanProjects/GymData/DataFolder/strong.csv'

df = pd.read_csv(strong_path)

# Delete the first 6 rows that contain useless data
df = df.iloc[6:].reset_index(drop=True)

# Remove unnecessary columns (we'll remove a couple more later)
col_to_drop = ['Notes', 'Workout Notes', 'RPE']
df.drop(columns = col_to_drop, inplace=True)

# Create a proper datetime column
df['datetime'] = pd.to_datetime(df['Date'])
df.drop(columns = 'Date', inplace=True)

# Rename the columns
df.columns = ['workout_name','duration', 'exercise_name', 'set_order', 'weight', 'reps', 'distance',
              'seconds', 'datetime']

# Create separate DFs for running and lifting

run_df = df[df.exercise_name=='Running']
lift_df = df[df.exercise_name!='Running']
lift_df.drop(columns = ['distance', 'seconds'], inplace=True)

# Create run, lift, and workout indices 

lift_df['lift_index'] = range(1, len(lift_df) + 1)
run_df['run_index'] = range(1, len(run_df) + 1)
lift_df['workout_id'] = lift_df.groupby('datetime').ngroup() + 1

# Reorder columns to my liking

lift_df=lift_df[['lift_index','workout_id', 'workout_name', 'duration', 'exercise_name', 
                 'set_order', 'weight', 'reps', 'datetime']]

run_df=run_df[['run_index','workout_name',
               'duration','exercise_name','distance','seconds','datetime']]

# Create metadata object to hold lifts table
metadata_obj = MetaData()

table_name = "workout_table"
run_table_name = 'running_table'


workout_table = Table(
    table_name,
    metadata_obj,
    Column('lift_index', Integer, primary_key=True),
    Column("workout_id", Integer, nullable=False),
    Column("workout_name", String(60), nullable=False),
    Column('duration', String(30), nullable = False),
    Column("exercise_name", String(60), nullable=False),
    Column("set_order", Integer, nullable=False),
    Column("weight", DECIMAL(precision=4, scale=1), nullable=False),
    Column("reps", Integer, nullable=False),
    Column("datetime", DateTime, nullable=False)
)

running_table = Table(
    run_table_name,
    metadata_obj,
    Column('run_index', Integer, primary_key=True),
    Column("workout_name", String(60), nullable=False),
    Column('duration', String(30), nullable = False),
    Column("exercise_name", String(60), nullable=False),
    Column("distance", DECIMAL(precision=4, scale=1), nullable=False),
    Column("seconds", Integer, nullable=False),
    Column("datetime", DateTime, nullable=False)
)

################### Create an SQLAlchemy engine
try:
    connection_url = "mysql+mysqlconnector://root:*0Lander96@localhost/gym_data"
    engine = create_engine(connection_url)

    print('engine created')
except(ValueError, TypeError):
    print('error creating SQLAlchemy engine')


# Create the table Schema in MySQL, if it hasn't already been created
    
inspector = inspect(engine)
if not inspector.has_table('workout_table'):
    workout_table.create(engine)
    print("Table 'workout_table' created successfully.")
else:
    print("Table 'workout_table' already exists.")


if not inspector.has_table('running_table'):
    running_table.create(engine)
    print("Table 'running_table' created successfully.")
else:
    print("Table 'running_table' already exists.")




################### Append the DFs to the MySQL tables
try: 
    lift_df.to_sql(table_name, engine, if_exists="replace", index=False)

    print('successful upload to MySQL')
except(ValueError, TypeError):
    print('error uploading df to MySQL')


try: 
    run_df.to_sql(run_table_name, engine, if_exists="replace", index=False)

    print('successful upload to MySQL')
except(ValueError, TypeError):
    print('error uploading df to MySQL')
# SPARKIFY DataLake (Amazon s3 - AWS EMR)

## Table of Contents

- [SPARKIFY DataLake (Amazon s3 - AWS EMR)](#sparkify-datalake-amazon-s3---aws-emr)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Tools](#tools)
  - [Schema](#schema)
    - [Fact Table](#fact-table)
      - [songplays](#songplays)
    - [Dimension Tables](#dimension-tables)
      - [users](#users)
      - [songs](#songs)
      - [artists](#artists)
      - [time](#time)

## Introduction

A music streaming startup, **Sparkify**, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I had to build building an ETL pipeline that extracts their data from **S3**, processes them using **Spark**, and loads the data back into **S3** as a set of dimensional tables in **parquet** format. This will allow their analytics team to continue finding insights in what songs their users are listening to.

We'll deploy this Spark process on a cluster using AWS.

## Tools

<p style="float:left">
<img src='./images/python.svg' alt="Python" title="Python"/>
<img src="./images/EMR.png" alt="EMR" title="EMR" width="50" height="50">
<img src="./images/spark-logo-hd.png" alt="Spark" title="spark" width="50" height="50">
<img src='./images/aws-s3.png' alt="s3" title="s3"/>
</p>
<div style="clear:both">



## Schema

Schema for Song Play Analysis

Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables:-

### Fact Table

#### songplays

- Records in log data associated with song plays i.e. records with page _NextSong_

- Columns
  - songplay_id
  - start_time
  - user_id
  - level
  - song_id
  - artist_id
  - session_id
  - location
  - user_agent

### Dimension Tables

#### users

- Users in the app
- Columns
  - user_id
  - first_name
  - last_name
  - gender
  - level

#### songs

- Songs in music database
- Columns
  - song_id
  - title
  - artist_id
  - year
  - duration

#### artists

- Artists in music database
- Columns
  - artist_id
  - name
  - location
  - lattitude
  - longitude

#### time

- timestamps of records in songplays broken down into specific units
- Columns
  - start_time
  - hour
  - day
  - week
  - month
  - year
  - weekday






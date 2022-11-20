#Baby Names fun project

This is a fun weekend project to evaluate my data engineering skills. It takes two Kaggle datasets (names of newborns in the US and list of Jewish names) and turns them into a well structured dashboard. 

Among other things, solution uses under the hood these buzzwords:
- Kimball's like architecture of a data warehouse (think of dimensional modelling, separate data layers for staging/integrated, change data capture)
- Everything in Docker Containers. Just git clone and docker compose to run.
- Airflow for orchestration
- PowerBI for dashboards
- Postgres database for databasing and the T from ETL
- Python for downloading, connecting and moving stuff here and there. The EL from ETL. Also there is pandas for a little bit of analysis.


#How to run
##Prerequisities:
1) Install git
2) Install Docker and Docker Compose. Docs here: https://docs.docker.com/engine/install/ubuntu/#install-using-the-repository
3) When running the app on Windows and WSL as I do, it's good to:
- Upgrade WSL to version 2 (required by Docker Compose). More read here: https://docs.docker.com/desktop/windows/wsl/
- Give Docker 8GB ram (4GB requirement for Airflow + something for a smooth run) and some processors
  - More read here: https://mrakelinggar.medium.com/set-up-configs-like-memory-limits-in-docker-for-windows-and-wsl2-80689997309c
  - tl;dr: copy provided .wslconfig to you %UserProfile% folder (so for example C:\[user]]\Michal). Then restart Docker Desktop and wsl.
- Install Docker Desktop. Sometimes it freezes but it's beautiful. 
- Tested on Windows 10 and WSL2 Ubuntu 20.04.5 LTS


##Configure:
All the configurations are availiable in config folder. There are separate jsons for the directory structure, db credentials and Kaggle key. These generally match whatever is in docker-compose.yml.

##Run Infra:
`mkdir babynames`

`cd babynames`

git clone:

`git clone TODO`

docker compose

`docker compose -f docker-compose.yml up`

##Access
There are 3 accessible services and 1 report:

**Airflow frontend:** http://localhost:8080

{ user: airflow, pass: airflow }

**Airflow postgres:** http://localhost:5432

{ user: airflow, pass: airflow, database: airflow }

**DWH postgres:** http://localhost:5433

{ user: michal, pass: reverse12, database: babynames }

**PowerBI:** https://app.powerbi.com/view?r=eyJrIjoiMTcwNGE3NzktMWVhMi00MGQ1LWFhNTAtMGU3NWM5YzU5NDQ5IiwidCI6ImZlNzA1M2M1LTJlNmYtNGU5My1iYTQ4LTE5NzI2YWRkZjY3ZSIsImMiOjl9
(public static link, automation works only through PowerBI gateway)

#Discussion for individuals steps
##Datasets
**US Baby Names** (https://www.kaggle.com/datasets/kaggle/us-baby-names)
- It tracks trends in how babies born in the US are named
- 2 granularities, on State a National level
- National starts in 1879, State in 1910
- in order to have consistent integrated data layer and as much data as possible, we have to combine them within the transformation
- also we should check if they are consistent (they appear so, in progress analysis can be found in *analysis* folder. Discrepancies come from either different beginnings or the fact that rows with less than 5 record are dropped due to anonymization)
- Note that analysis is in progress, it'd be definitely beneficial to spin up a Jupyter and present it notebooks, but this I save for another day 

**Jewish Baby Names**(https://www.kaggle.com/datasets/netanel246/jewish-baby-names)
- Extra dataset so the US Babies don't feel alone
- used to distinguish if the name is Jewish
- we could definitely find more datasets like this to futher enrich Name origin dimension

##Data download
- is responsible for moving the data from Kaggle server, over internal landing folders into a *raw* in Postgres 
- since the dataset is updated very rarery, we first check if the previously downloaded files have the same md5 hash as freshly download md5s from Kaggle
- this not only prevents us to download the files again for no reason (aka simple change data capture)
- but also neatly checks data integrity after the files are automatically unzipped files
- if storage was a concern (oved data validity), one could definitely load the files without unzipping (pandas.read_csv() can read zipped csv, too)
- also various backups and file versionings could be put in place (I love such scripts so much, they have saved my a** so many times!), but let's save those for projects beyond static Kaggle dataset
- custom utility created for this in *utils* folder
- there is also prepared space for: 
  - possible preprocess of the data in pandas (which wasn't really necessary in this instance)
  - and also for **input data testing**. I like to used library called Pandera for this purpose, but weekend is short so I'll save this for another iteration

##Data transform
- is responsible for moving the data *raw* in Postgres into the integrated DHW layer
- integrated DHW layer is designed according to Kimball's principles of dimensional modelling: fact table (NameCounts) with the metrics (Count of name occurance) in the middle, dimensions (state, name, date, gender) on the outside as beams of a Star :)  
- SQL queries loaded trough custom utility, which mostly encapsulates the database conection (visible in *utils* folder)
- DDL scripts in sql init file, ran by docker compose
- DDLs also includes static data, which are unlike to change: Genders and State names (this is a slight extension of the dataset, PowerBI's map only works with full state names) 
- DDLs also includes creation of indices. These are important for reasonable data read and write times
- In further versions this should handle Slowly Changing Dimensions in order to track changes in integrated layer

##Dashboards
- is responsible for moving the data from integrated DHW layer into PowerBI dashboards
- PowerBI dashboard is available at public static link https://app.powerbi.com/view?r=eyJrIjoiMTcwNGE3NzktMWVhMi00MGQ1LWFhNTAtMGU3NWM5YzU5NDQ5IiwidCI6ImZlNzA1M2M1LTJlNmYtNGU5My1iYTQ4LTE5NzI2YWRkZjY3ZSIsImMiOjl9
- Answer questions about trending names and number of newborns in states
- Various visualizations include over the time metrics and geo visualization
- Filterable on all dimensions (state, name, gender, data, jewish name origin)
- updating works locally or through PowerBI gateway (Windows computer needed!)
- timely reduced .pbix file is in *powerbi* folder, please note that you have to install PowerBI Desktop to see the local version (free, but Windows only). On other platforms, please use online version. 

##How to run
either in Airflow frontend, pipeline *babies* or in bash:

``
docker ps #locate CONTAINER_ID of a worker
``

``
docker exec -it [CONTAINER_ID of a worker]  bash #connect to the container
``

``
airflow dags test babies #run the dag in console
``

pipeline script is called babies and is located in folder *dags*


##Future
There are several things that I've left for future iterations of the project. These include:
- Input data testing: Check if the input data are in good enough shape to load. Various method can be used on various places, I'd started with my favorite library pandera and checked the dataframe prior loading to the raw database layer
- unit testing: simularly to previous topic, code and tools should be tested
- Well presented analysis: Input data were checked only quick and dirty, spinning up another container with Jupyter so the analysis is well presented would be beneficial
- Only simple logging module and build in airflow stuff are used. Failed jobs should send notifications, for example via Slack API
- Tracking history in integrated data layer trough slowly changing dimensions
- Devops configuration: Postgres and Airflow containers would use some effort for setting up before going to the production. Especially for credentials, Docker secrets would be the next step.
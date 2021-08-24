## AWS Redshift Cloud Data Warehouse - Sparkify

<br>

**Author:** Darren Foley
**Email:** darren.foley@ucdconnect.ie
**Date:** 2021-08-24

<br>

### Project Description and setup

| FileName             | Type             | Description                                      |
|---------------------:|:----------------:|:------------------------------------------------:|
| create_tables.py     | python           | Script to create tables in Redshift              |
| create_warehouse.py  | python           | Iac script to create warehouse                   |
| delete_warehouse.py  | python           | Iac script to delete warehouse                   |
| song_dwh.cfg         | config           | Sample configuration File                        |
| etl.py               | python           | ETL Call Script                                  |
| README.md            | markdown         | README file                                      |
| run.sh               | shell            | Shell wrapper to create and populate warehouse   |
| sql_queries.py       | python           | Script that contains all SQL                     |
| test.ipynb           | python notebook  | Test python notebook                             |
| update_git.sh        | shell            | Shell script to update local git                 |

<br>

<p>The project song_dwh.cfg file will need to be set up before scripts can be called. Create the admin AWS user who will spin up the Redhift Cluster, add the key and secret to song_dwh.cfg. The endpoint and ARN user will be populated in song_dwh.cfg automatically when the create_database.py script is called.</p>

<br>

You can run the scripts individually like so:

```
> python create_warehouse.py
......

```

Then run the create tables

```
> python create_tables.py
.....
```

Followed by the ETL

```
> python etl.py
```

or alternativily you can run the shell wrapper run.sh like so

```
> ./run.sh
```

<br>

To destroy the Redhsift cluster after use

```
> python delete_warehouse.py
```

<br>

### Project Outline

<br>

<p>Due to the increase in demand of sparkify services and an increase in demand for analytics within the business, sparkify have decided to move their Enterprise data warehouse to the cloud. AWS Redshift was chosen as the DWH technology of choice due to the simularity with their current on premise Postgres instance. Redshifts MPP architecture should significantly improve read performance of day to day reports and queries, provided the appropriate distribution key is chosen.</p> 

<br>

The project is broken into the following sections:

1. IaC Design
2. Table and Schema Design
3. ETL Design
4. Sample Queries/Reports

<br>

#### 1. IaC Design

<br>

<p></p>
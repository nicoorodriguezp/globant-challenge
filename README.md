# Globant’s Data Engineering Coding Challenge

## Repository content:
The repository contains all of the API code necessary to address the challenge. Full documentation detailing the challenge is provided towards the end of this document.

Developed by Nicolas Gaston Rodriguez Perez - January 2024.

## Technology stack:

The API has been developed using the Python programming language. This implementation enables its deployment through the Flask framework, also leveraging Apache Spark to perform read and write operations on the corresponding tables.

The development of the challenge relied on an Amazon S3 Bucket and the cloud service offered by Snowflake, facilitating the creation of a Lakehouse architecture. This choice ensures long-term scalability according to the requirements of the project.

## Lakehouse structure:
The main database within the lakehouse is called GLOBANT and encompasses three essential schemas representing different layers of data management:

- **GLOBANT.RAW**: This layer holds the raw, unprocessed data.
- **GLOBANT.TRUSTED**: The data in this layer has been validated, ensuring its reliability and integrity.

Although the challenge specifically uses the first two layers, the possibility of incorporating the "REFINED" layer - which houses modeled and refined data - is pending for future enhancements.

## API Reference

#### LOAD HIRED EMPLOYEES ENDPOINT
```http
  GET /employees/load_hired_employees/
```

| STATUS | MESSAGE  | STATUS CODE  |
| :----- | :------- | :----------- |
| `OK` | `The historical data of hired employees has been successfully uploaded to Snowflake.` | `200` |
| `ERROR` | `No data loaded from the CSV file.` | `404` |
| `ERROR` | `An error occurred while loading the historical data of hired employees to Snowflake.` | `500` |

Extracts the hired employees data from the hired_employees.csv file stored in an S3 bucket, loads it into the **GLOBANT.RAW.HIRED_EMPLOYEE** table and then loads it into **GLOBANT.TRUSTED.HIRED_EMPLOYEE** with the necessary transformations if any.

In case the file does not exist or is empty, it will return the corresponding error message, otherwise it will return status code 200 with the confirmation message.



#### LOAD DEPARTMENTS ENDPOINT
```http
  GET /departments/load_departments/
```

| STATUS | MESSAGE  | STATUS CODE  |
| :----- | :------- | :----------- |
| `OK` | `The historical data of departments has been successfully uploaded to Snowflake.` | `200` |
| `ERROR` | `No data loaded from the CSV file.` | `404` |
| `ERROR` | `An error occurred while loading the historical data of departments to Snowflake.` | `500` |

Extracts the data of the hired employees from the departments.csv file stored in an S3 bucket, loads it into the **GLOBANT.RAW.DEPARTMENT** table and then loads it into **GLOBANT.TRUSTED.DEPARTMENT** with the necessary transformations if any.

In case the file does not exist or is empty, it will return the corresponding error message, otherwise it will return status code 200 with the confirmation message.



#### LOAD JOBS ENDPOINT
```http
  GET /jobs/load_jobs/
```

| STATUS | MESSAGE  | STATUS CODE  |
| :------| :------- | :----------- |
| `OK` | `The historical data of jobs has been successfully uploaded to Snowflake.` | `200` |
| `ERROR` | `No data loaded from the CSV file.` | `404` |
| `ERROR` | `An error occurred while loading the historical data of jobs to Snowflake.` | `500` |

Extracts the data of the hired employees from the jobs.csv file stored in an S3 bucket, loads it into the **GLOBANT.RAW.JOB** table and then loads it into **GLOBANT.TRUSTED.JOB** with the necessary transformations if any.

In case the file does not exist or is empty, it will return the corresponding error message, otherwise it will return status code 200 with the confirmation message.



#### REPORT OF EMPLOYEES HIRED BY DEPARTMENT AND POSITION 

```http
  GET /reports/employees_hired_for_each_job_and_department_divided_by_quarter/
```
```http
  GET /reports/employees_hired_for_each_job_and_department_divided_by_quarter/<int:year>/
```

| Parameter | Type  | Description                 |
| :-------- | :---- | :-------------------------- |
| `YEAR`    | `int` | **OPTIONAL**. Year to fetch |

Returns a JSON with the number of employees hired for each position and department in the last year by parameter, divided by quarter and sorted alphabetically by department and position.

| STATUS    | MESSAGE  | RESPONSE | STATUS CODE |
| :-------- | :------- | :------- | :---------- |
| `OK` | `Number of employees hired for each job and department in {YEAR}, divided by quarter, and ordered alphabetically by department and job.` | `JSON` | `200` |
| `ERROR` | `No data obtained for the year entered.` | - | `404` |

In case the parameter is not sent, it returns the data for 2021.

#### Response `JSON` example:
    "response": [
      {
        "DEPARTMENT": "Accounting",
        "JOB": "Account Representative IV",
        "Q1": 1,
        "Q2": 0,
        "Q3": 0,
        "Q4": 0
      },
      ...
    ]


#### REPORT OF DEPARTMENTS THAT HIRED MORE THAN THE AVERAGE NUMBER OF HIRES AMONG ALL DEPARTMENTS.

```http
  GET /reports/department_that_hired_more_employees_than_the_mean_of_employees_hired/
```
```http
  GET /reports/department_that_hired_more_employees_than_the_mean_of_employees_hired/<int:year>/
```

| Parameter | Type  | Description                 |
| :-------- | :---- | :-------------------------- |
| `YEAR`    | `int` | **OPTIONAL**. Year to fetch |

Returns a JSON with a list of ID, NAME and TOTAL number of employees hired for each department that exceeded the average number of hires in the past year by parameter across all departments, sorted in descending order by number of employees hired.

| STATUS    | MESSAGE  | RESPONSE | STATUS CODE |
| :-------- | :------- | :------- | :---------- |
| `OK` | `List of ID, NAME and TOTAL number of employees hired for each department that exceeded the average number of hires in {YEAR} across all departments, sorted in descending order by number of employees hired.` | `JSON` | `200` |
| `ERROR` | `No data obtained for the year entered.` | - | `404` |

In case the parameter is not sent, it returns the data for 2021.

#### Response `JSON` example:
    "response":[
        {
          "DEPARTMENT": "Support",
          "HIRED": 221,
          "ID": 8
        },
        ...
    ]

#### API HEALTH CHECK

```http
  GET /healthcheck
```

| STATUS | MESSAGE                      | STATUS CODE  |
| :------| :--------------------------- | :----------- |
| `OK`   | `The API is up and running.` | `200`        |

Returns status code 200 if the API is running.

## Environment Variables

To run this project, you will need to include the following environment variables in your .env file.

#### AWS CREDENTIALS
`AWS_ACCESS_KEY_ID`

`AWS_SECRET_ACCESS_KEY`

#### SNOWFLAKE CREDENTIALS
`SNOWFLAKE_ACCOUNT_URL`

`SNOWFLAKE_ACCOUNT`

`SNOWFLAKE_REGION`

`SNOWFLAKE_WAREHOUSE`

`SNOWFLAKE_DATABASE`

`SNOWFLAKE_ROLE`

`SNOWFLAKE_USER`

`SNOWFLAKE_PWD`

## Deployment

### Build the Docker image:

```bash
  docker build -t globant-challenge .
```

### Start the Docker container:
```bash
  docker run -p 5000:5000 --name globant-challenge-container globant-challenge
```
## Authors

- Nicolas Gaston Rodriguez Perez [@nicoorodriguezp](https://www.github.com/nicoorodriguezp)


## Documentation

[Challenge requirements](https://github.com/nicoorodriguezp/globant-challenge/blob/main/documentation/Globant%E2%80%99s_Data_Engineering_Coding_Challenge.pdf)

[Challenge Files](https://github.com/nicoorodriguezp/globant-challenge/tree/main/documentation/csv)


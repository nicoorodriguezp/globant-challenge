import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as sf
from snowflake.connector import connect
import json

load_dotenv()
SNOWFLAKE_ACCOUNT_URL = str(os.getenv('SNOWFLAKE_ACCOUNT_URL'))
SNOWFLAKE_ACCOUNT     = str(os.getenv('SNOWFLAKE_ACCOUNT'))
SNOWFLAKE_WAREHOUSE   = str(os.getenv('SNOWFLAKE_WAREHOUSE'))
SNOWFLAKE_DATABASE    = str(os.getenv('SNOWFLAKE_DATABASE'))
SNOWFLAKE_REGION      = str(os.getenv('SNOWFLAKE_REGION'))
SNOWFLAKE_ROLE        = str(os.getenv('SNOWFLAKE_ROLE'))
SNOWFLAKE_USER        = str(os.getenv('SNOWFLAKE_USER'))
SNOWFLAKE_PWD         = str(os.getenv('SNOWFLAKE_PWD'))

SNOWFLAKE_JDBC_JAR      = "/drivers/snowflake_driver/snowflake-jdbc-3.13.30.jar"
SNOWFLAKE_SOURCE_NAME   = 'net.snowflake.spark.snowflake'
spark = SparkSession.builder \
    .appName("LoadToSnowflake") \
    .config("spark.driver.extraClassPath", SNOWFLAKE_JDBC_JAR) \
    .config("spark.jars", SNOWFLAKE_JDBC_JAR) \
    .getOrCreate()

def get_csv_from_bucket(file_name, file_schema, s3_bucket_name = 'globant-challenge-api', folder_path='raw'):
    '''
        Retrieves a csv file from Amazon S3 bucket.
        Params:
            - file_name
            - file_schema
            - s3_bucket_name
            - folder_path\n
        Returns:
            - Spark Data Frame
    '''
   
    AWS_ACCESS_KEY_ID       = str(os.getenv('AWS_ACCESS_KEY_ID'))
    AWS_SECRET_ACCESS_KEY   = str(os.getenv('AWS_SECRET_ACCESS_KEY'))
    try:
        s3          = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        file_key    = f'{folder_path}/{file_name}'
        obj         = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
        csv_content = obj['Body'].read().decode('utf-8')

        rdd = spark.sparkContext.parallelize(csv_content.splitlines())
        df  = spark.read.csv(rdd, header=False, schema=file_schema)
        df.printSchema()
        return df
    except:
        print(f'File:{s3_bucket_name}:{file_key} was not found.')
        return df

def execute_statement_snowflake(snowflake_query):
    '''
        Execute a statement in Snowflake. It does not return the result of the executed sentence.\n

        Params:
        - snowflake_query: Statement or query to be executed in Snowflake.
    '''
    conn = connect(
        account     = SNOWFLAKE_ACCOUNT,
        user        = SNOWFLAKE_USER,
        password    = SNOWFLAKE_PWD,
        warehouse   = SNOWFLAKE_WAREHOUSE,
        database    = SNOWFLAKE_DATABASE
    )

    cursor = conn.cursor()
    
    cursor.execute(snowflake_query)
    
    cursor.close()
    conn.close()

def df_batch_insert_snowflake(df, table_name, snowflake_schema = 'RAW', max_records_per_partition = 1000):
    '''
        Receives a Data Frame from Spark, splits it into partitions of up to {max_records_per_partition} records and then 
        does sequential loads to the {snowflake_schema}.{table_name} table in batches, where each batch is a partition. \n

        Params:
        - df: Spark DF
        - table_name: Name of the table to be loaded
        - snowflake_schema: Layer of the Lakehouse where the table to be loaded is located. Ex. "RAW"
        - max_records_per_partition: Maximum number of records that each batch can contain
    '''

    snowflake_jdbc_url  = f"jdbc:snowflake://{SNOWFLAKE_ACCOUNT_URL}/?warehouse={SNOWFLAKE_WAREHOUSE}&db={SNOWFLAKE_DATABASE}&schema={snowflake_schema}"
    snowflake_db_properties = {
        "user":     SNOWFLAKE_USER,
        "password": SNOWFLAKE_PWD,
        "driver":   "net.snowflake.client.jdbc.SnowflakeDriver"
    }

    windowSpec = Window.orderBy(sf.monotonically_increasing_id())
    df = df.withColumn("ROW_COUNT_DF", sf.row_number().over(windowSpec)) # Ensure that there is a sequential ID for each DF record.
    df = df.withColumn(
        "PARTITION_ID", ((sf.col("ROW_COUNT_DF") - 1) / max_records_per_partition).cast("int") # Create partitions of at most {max_records_per_partition} records
    )
    df = df.drop('ROW_COUNT_DF')

    partition_count = df.select("PARTITION_ID").distinct().count()
    print(f'Se crearon {partition_count} particiones de hasta {max_records_per_partition} registros.')

    # Insert up to {max_records_per_partition} records per query
    for partition in range(partition_count):
        partition_df = df.filter(sf.col("PARTITION_ID") == partition).drop("PARTITION_ID") 

        if not partition_df.isEmpty():
            partition_df.write.jdbc(url=snowflake_jdbc_url, table=table_name, mode="append", properties=snowflake_db_properties)
            print(f'{partition_df.count()} records were loaded to table {SNOWFLAKE_DATABASE}.{snowflake_schema}.{table_name}')
            df = df.filter(sf.col("PARTITION_ID") != partition) # Those that have already been loaded are deleted to remove them from memory

    del df

def read_snowflake_data_json(snowflake_query, snowflake_schema = 'TRUSTED'):
    '''
        Params:
            - snowflake_query \n
            - snowflake_schema: Schema where the table to be accessed is located. Represents the Lakehouse layer where the table is located. \n
        Returns:
            A list with the result of the query in json format. \n
    '''
    snowflake_jdbc_url = f"jdbc:snowflake://{SNOWFLAKE_ACCOUNT_URL}/?warehouse={SNOWFLAKE_WAREHOUSE}&db={SNOWFLAKE_DATABASE}&schema={snowflake_schema}&user={SNOWFLAKE_USER}&password={SNOWFLAKE_PWD}"

    df = spark.read \
    .format("jdbc") \
    .option("url", snowflake_jdbc_url) \
    .option("query", snowflake_query) \
    .load()

    if df:
        response_objects = [json.loads(record) for record in df.toJSON().collect()]
        return  response_objects
    else:
        return None

def get_employees_hired_for_each_job_and_department_divided_by_quarter(year = 2021, snowflake_schema = 'TRUSTED'):
    '''
        Params:
            - year\n
            - snowflake_schema\n
        Returns: 
        List of the number of employees hired for each position and department in {year} divided by quarter in JSON format. The response will be sorted alphabetically by department and position.
    '''
    snowflake_query = f'''
        SELECT 
            NVL(DEPARTMENT, 'UNKNOWN') AS DEPARTMENT,
            NVL(JOB, 'UNKNOWN') AS JOB,
            SUM(CASE WHEN DATE_PART('quarter', DATETIME) = 1 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN DATE_PART('quarter', DATETIME) = 2 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN DATE_PART('quarter', DATETIME) = 3 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN DATE_PART('quarter', DATETIME) = 4 THEN 1 ELSE 0 END) AS Q4
        FROM 
            GLOBANT.TRUSTED.HIRED_EMPLOYEE HE
            LEFT JOIN GLOBANT.TRUSTED.JOB           J ON (HE.JOB_ID = j.ID)
            LEFT JOIN GLOBANT.TRUSTED.DEPARTMENT    D ON (HE.DEPARTMENT_ID = d.ID)
        WHERE 
            DATETIME BETWEEN '{year}-01-01' AND '{year}-12-31'
        GROUP BY 
            DEPARTMENT,
            JOB
        ORDER BY 
            DEPARTMENT,
            JOB
    '''
    
    return read_snowflake_data_json(snowflake_query, snowflake_schema)
            
def get_department_that_hired_more_employees_than_the_mean(year = 2021, snowflake_schema = 'TRUSTED'):
    '''
        Params:
            - year\n
            - snowflake_schema\n
        Returns: 
        List of ID, NAME and TOTAL number of employees hired for each department that exceeded the average number of hires in {year} across all departments, sorted in descending order by number of employees hired.
    '''
    snowflake_query = f'''
        WITH 
            DEPARTMENT_EMPLOYEE_COUNT AS (
                SELECT
                    D.ID            AS DEPARTMENT_ID,
                    D.DEPARTMENT    AS DEPARTMENT_NAME,
                    COUNT(HE.ID)    AS HIRED
                FROM
                    GLOBANT.TRUSTED.DEPARTMENT D
                    INNER JOIN GLOBANT.TRUSTED.HIRED_EMPLOYEE HE ON (D.ID = HE.DEPARTMENT_ID)
                WHERE
                    DATE_PART(YEAR, HE.DATETIME) = {year}
                GROUP BY
                    D.ID, D.DEPARTMENT
            ),
            MEAN_EMPLOYEE_COUNT AS (
                SELECT
                    AVG(HIRED) AS MEAN_HIRED
                FROM
                    DEPARTMENT_EMPLOYEE_COUNT
            )
        SELECT
            DEC.DEPARTMENT_ID   AS ID,
            DEC.DEPARTMENT_NAME AS DEPARTMENT,
            DEC.HIRED           AS HIRED
        FROM
            DEPARTMENT_EMPLOYEE_COUNT DEC
            LEFT JOIN MEAN_EMPLOYEE_COUNT MEC ON (DEC.HIRED > MEC.MEAN_HIRED)
        ORDER BY
            DEC.HIRED DESC
    '''
    
    return read_snowflake_data_json(snowflake_query, snowflake_schema)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType 


class Department:
    def __init__(self, id, department):
        self.id = id
        self.department = department

    def get_table_scheme():
        return StructType([
            StructField("ID", IntegerType(), True),
            StructField("DEPARTMENT", StringType(), True)
        ])
    
    def get_query_for_loading_into_trusted_layer():
        return '''
            INSERT INTO GLOBANT.TRUSTED.DEPARTMENT (ID, DEPARTMENT)
            SELECT 
                D.ID,
                D.DEPARTMENT
            FROM 
                GLOBANT.RAW.DEPARTMENT D
            WHERE NOT EXISTS (
                SELECT 1
                FROM 
                    GLOBANT.TRUSTED.DEPARTMENT AS DT
                WHERE 
                    D.ID = DT.ID 
            );
        '''


class HiredEmployee:
    def __init__(self, id, name, datetime, department_id, job_id):
        self.id = id
        self.name = name
        self.datetime = datetime
        self.department_id = department_id
        self.job_id = job_id

    def get_table_scheme():
        return StructType([
            StructField("ID", IntegerType(), True),
            StructField("NAME", StringType(), True),
            StructField("DATETIME", StringType(), True),
            StructField("DEPARTMENT_ID", IntegerType(), True),
            StructField("JOB_ID", IntegerType(), True),
        ])
    
    def get_query_for_loading_into_trusted_layer():
        return '''
            INSERT INTO GLOBANT.TRUSTED.HIRED_EMPLOYEE (ID, NAME, DATETIME, DEPARTMENT_ID, JOB_ID)
            SELECT 
                ID,
                NAME,
                TO_TIMESTAMP(DATETIME) AS DATETIME,
                DEPARTMENT_ID,
                JOB_ID
            FROM 
                GLOBANT.RAW.HIRED_EMPLOYEE HE
            WHERE NOT EXISTS (
                SELECT 1
                FROM 
                    GLOBANT.TRUSTED.HIRED_EMPLOYEE AS HET
                WHERE 
                    HE.ID = HET.ID 
            );

        '''


class Job:
    def __init__(self, id, job):
        self.id = id
        self.job = job
    
    def get_table_scheme():
        return StructType([
            StructField("ID", IntegerType(), True),
            StructField("JOB", StringType(), True)
        ])
    
    def get_query_for_loading_into_trusted_layer():
        return '''
            INSERT INTO GLOBANT.TRUSTED.JOB (ID, JOB)
            SELECT 
                J.ID,
                J.JOB
            FROM 
                GLOBANT.RAW.JOB J
            WHERE NOT EXISTS (
                SELECT 1 
                FROM 
                    GLOBANT.TRUSTED.JOB AS JT
                WHERE 
                    J.ID = JT.ID 
            );
        '''
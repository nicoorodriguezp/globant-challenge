import os
import unittest
import requests
import boto3
from botocore.exceptions import NoCredentialsError
from snowflake.connector import connect
from dotenv import load_dotenv

class TestAPI(unittest.TestCase):
    load_dotenv()

    def test_healthcheck_endpoint(self):
        url = 'http://localhost:5000/healthcheck/'
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        print('Health Check --> OK.')

    def test_s3_connection(self):
        try:
            s3 = boto3.client('s3', aws_access_key_id=str(os.getenv('AWS_ACCESS_KEY_ID')), aws_secret_access_key=str(os.getenv('AWS_SECRET_ACCESS_KEY')))
            response = s3.list_buckets()
            self.assertIsNotNone(response)
            print("Successful connection to the S3 bucket. --> OK")

        except NoCredentialsError:
            print("Invalid credentials. Could not connect to the S3 bucket. --> ERROR")

        except Exception as e:
            print(f"Error connecting to bucket S3: {str(e)} --> ERROR")

    def test_snowflake_connection_and_query_execution(self):

        snowflake_query = "SELECT 1"  
        try:
            
            conn = connect(
                account     = str(os.getenv('SNOWFLAKE_ACCOUNT')),
                user        = str(os.getenv('SNOWFLAKE_USER')),
                password    = str(os.getenv('SNOWFLAKE_PWD')),
                warehouse   = str(os.getenv('SNOWFLAKE_WAREHOUSE')),
                database    = str(os.getenv('SNOWFLAKE_DATABASE'))
            )

            cursor = conn.cursor()
            cursor.execute(snowflake_query)

            result = cursor.fetchall()

            cursor.close()
            conn.close()

            self.assertTrue(len(result) > 0, "There was an error when querying to Snowflake. --> ERROR")
            print("Snowflake connection and query execution successful. --> OK")
        except Exception as e:
            print(f"Error when connecting to Snowflake: {str(e)}")
            self.fail("Snowflake connection and query execution failed.")

    def test_load_hired_employees_endpoint(self):
        url = 'http://localhost:5000/employees/load_hired_employees/'
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        print('Load hired employees --> OK.')
    
    def test_load_departments_endpoint(self):
        url = 'http://localhost:5000/departments/load_departments/'
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        print('Load departments --> OK.')
    
    def test_load_jobs_endpoint(self):
        url = 'http://localhost:5000/jobs/load_jobs/'
        response = requests.get(url)
        self.assertEqual(response.status_code, 200)
        print('Load jobs --> OK.')

    def test_get_employees_hired_for_each_job_and_department_divided_by_quarter_endpoint(self):
        url = 'http://localhost:5000/reports/employees_hired_for_each_job_and_department_divided_by_quarter/2021/'
        response = requests.get(url)

        self.assertEqual(response.status_code, 200)

        # Check if the structure of the API response is correct.
        api_response = response.json()
        self.assertTrue("message"   in api_response)
        self.assertTrue("response"  in api_response)
        self.assertTrue("status"    in api_response)

        response_expected_keys = ["DEPARTMENT", "JOB", "Q1", "Q2", "Q3", "Q4"]
        for index, item in enumerate(api_response["response"]):

            if not all(key in item for key in response_expected_keys):
                print(f"Element {index + 1} does not have the expected structure: {item}")

        print('Report of employees hired for each job and department divided by quarter --> OK.')

    def test_get_department_that_hired_more_employees_than_the_mean_of_employees_hired_endpoint(self):
        url = 'http://localhost:5000/reports/department_that_hired_more_employees_than_the_mean_of_employees_hired/2021/'
        response = requests.get(url)

        self.assertEqual(response.status_code, 200)

        # Check if the structure of the API response is correct.
        api_response = response.json()
        self.assertTrue("message"   in api_response)
        self.assertTrue("response"  in api_response)
        self.assertTrue("status"    in api_response)

        response_expected_keys = ["DEPARTMENT", "HIRED", "ID"]
        for index, item in enumerate(api_response["response"]):

            if not all(key in item for key in response_expected_keys):
                print(f"Element {index + 1} does not have the expected structure: {item}")
        
        print('Report of departments that hired more employees than the mean of employees hired --> OK.')

if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestAPI('test_healthcheck_endpoint'))
    suite.addTest(TestAPI('test_s3_connection'))
    suite.addTest(TestAPI('test_snowflake_connection_and_query_execution'))
    suite.addTest(TestAPI('test_load_hired_employees_endpoint'))
    suite.addTest(TestAPI('test_load_departments_endpoint'))
    suite.addTest(TestAPI('test_load_jobs_endpoint'))
    suite.addTest(TestAPI('test_get_employees_hired_for_each_job_and_department_divided_by_quarter_endpoint'))

    unittest.TextTestRunner(verbosity=2).run(suite)

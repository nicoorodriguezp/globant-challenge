from flask import Flask, jsonify
import logging
from controller import df_batch_insert_snowflake, execute_statement_snowflake, get_csv_from_bucket, get_department_that_hired_more_employees_than_the_mean, get_employees_hired_for_each_job_and_department_divided_by_quarter as get_hired_employees_by_quarter, read_snowflake_data_json
from models import HiredEmployee, Department, Job

app = Flask(__name__)

@app.route('/healthcheck/', methods=['GET'])
def healthcheck():
    return jsonify({'status': 'OK', 'message': 'The API is up and running.'}), 200

# ELT
@app.route('/employees/load_hired_employees/', methods=['GET'])
def load_hired_employees():
    '''
        Extracts the hired employees data from the hired_employees.csv file stored in an S3 bucket, loads it into the GLOBANT.RAW.HIRED_EMPLOYEE table and then loads it 
        into GLOBANT.TRUSTED.HIRED_EMPLOYEE with the necessary transformations if any. \n

        In case the file does not exist or is empty, it will return the corresponding error message, otherwise it will return status code 200 with the confirmation message.
    '''
    try:
        df = get_csv_from_bucket(file_name = 'hired_employees.csv', file_schema = HiredEmployee.get_table_scheme(), s3_bucket_name = 'globant-challenge-api', folder_path='raw')

        if df:

            # Load to RAW layer using batches
            df_batch_insert_snowflake(df, 'hired_employee', 'RAW', max_records_per_partition = 1000)

            # Load to TRUSTED layer
            execute_statement_snowflake(HiredEmployee.get_query_for_loading_into_trusted_layer())
           
            return jsonify({'status': 'OK', 'message': 'The historical data of hired employees has been successfully uploaded to Snowflake.'}), 200
        else:
            return jsonify({'status': 'ERROR', 'message': 'No data loaded from the CSV file.'}), 404
    except Exception as e:
        logging.exception("Error loading hired employees data: %s", str(e))
        return jsonify({'status': 'ERROR', 'message': 'An error occurred while loading the historical data of hired employees to Snowflake.'}), 500

@app.route('/departments/load_departments/', methods=['GET'])
def load_departments():
    '''
        Extracts the data of the hired employees from the departments.csv file stored in an S3 bucket, loads it into the **GLOBANT.RAW.DEPARTMENT** table and then loads it 
        into **GLOBANT.TRUSTED.DEPARTMENT** with the necessary transformations if any. \n

        In case the file does not exist or is empty, it will return the corresponding error message, otherwise it will return status code 200 with the confirmation message.

    '''
    try:
        df = get_csv_from_bucket(file_name = 'departments.csv', file_schema = Department.get_table_scheme(), s3_bucket_name = 'globant-challenge-api', folder_path='raw')

        if df:
            # Load to RAW layer using batches
            df_batch_insert_snowflake(df, 'department', 'RAW', max_records_per_partition = 1000)

            # Load to TRUSTED layer
            execute_statement_snowflake(Department.get_query_for_loading_into_trusted_layer())

            return jsonify({'status': 'OK', 'message': 'The historical data of departments has been successfully uploaded to Snowflake.'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'No data loaded from the CSV file.'}), 404
    except Exception as e:
        logging.exception("Error loading departments data: %s", str(e))
        return jsonify({'status': 'ERROR', 'message': 'An error occurred while loading the historical data of departments to Snowflake.'}), 500

@app.route('/jobs/load_jobs/', methods=['GET'])
def load_jobs():
    '''
        Extracts the data of the hired employees from the jobs.csv file stored in an S3 bucket, loads it into the **GLOBANT.RAW.JOB** table and then loads it 
        into **GLOBANT.TRUSTED.JOB** with the necessary transformations if any. \n

        In case the file does not exist or is empty, it will return the corresponding error message, otherwise it will return status code 200 with the confirmation message.
    '''
    try:
        df = get_csv_from_bucket(file_name = 'jobs.csv', file_schema = Job.get_table_scheme(), s3_bucket_name = 'globant-challenge-api', folder_path='raw')

        if df:
            
            # Load to RAW layer using batches
            df_batch_insert_snowflake(df, 'job', 'RAW', max_records_per_partition = 1000)

            # Load to TRUSTED layer
            execute_statement_snowflake(Job.get_query_for_loading_into_trusted_layer())

            return jsonify({'status': 'OK', 'message': 'The historical data of jobs has been successfully uploaded to Snowflake.'}), 200
        else:
            return jsonify({'status': 'error', 'message': 'No data loaded from the CSV file.'}), 404
    except Exception as e:
        logging.exception("Error loading jobs data: %s", str(e))
        return jsonify({'status': 'ERROR', 'message': 'An error occurred while loading the historical data of jobs to Snowflake.'}), 500


# Reports
@app.route('/reports/employees_hired_for_each_job_and_department_divided_by_quarter/', defaults={'year': 2021}, methods=['GET'])
@app.route('/reports/employees_hired_for_each_job_and_department_divided_by_quarter/<int:year>/', methods=['GET'])
def get_employees_hired_for_each_job_and_department_divided_by_quarter(year):
    '''
        Params:
            - year: Ex. 2023 \n
        Returns:
            List of the number of employees hired for each position and department in {year} divided by quarter in JSON format.\n
            The response will be sorted alphabetically by department and position.\n

            Response Ex.\n
            [
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
    '''
    
    try:
        response = get_hired_employees_by_quarter(year)
        if response:
            return jsonify({
                'status': 'OK',
                'message':f'Number of employees hired for each job and department in {year}, divided by quarter, and ordered alphabetically by department and job.', 
                'response': response
            }), 200
        else:
            return jsonify({'status': 'ERROR', 'message': 'No data obtained for the year entered.'}), 404
    except Exception as e:
        logging.exception(f"Error loading number of employees hired for each job and department in {year} data: %s", str(e))
        return jsonify({'status': 'ERROR', 'message': 'There was an error when trying to obtain the report of employees hired by quarter.'}), 500


@app.route('/reports/department_that_hired_more_employees_than_the_mean_of_employees_hired/', defaults={'year': 2021}, methods=['GET'])
@app.route('/reports/department_that_hired_more_employees_than_the_mean_of_employees_hired/<int:year>/', methods=['GET'])
def get_department_that_hired_more_employees_than_the_mean_of_employees_hired(year):
    '''
        Params:
            - year: Ex. 2023 \n
        Returns:
            Returns a JSON with a list of ID, NAME and TOTAL number of employees hired for each department that exceeded the average number of hires in the past 
            year by parameter across all departments, sorted in descending order by number of employees hired. \n
            Response Ex.\n
            [
                {
                "DEPARTMENT": "Support",
                "HIRED": 221,
                "ID": 8
                },
                ...
            ]
    '''
    try:
        response = get_department_that_hired_more_employees_than_the_mean(year)
        if response:
            return jsonify({
                'status': 'OK',
                'message':f'List of ID, NAME and TOTAL number of employees hired for each department that exceeded the average number of hires in {year} across all departments, sorted in descending order by number of employees hired.', 
                'response': response
            }), 200
        else:
            return jsonify({'status': 'ERROR', 'message': 'No data obtained for the year entered.'}), 404
    except Exception as e:
        logging.exception(f"Error loading departments that exceeded the average number of hires in {year}: %s", str(e))
        return jsonify({'status': 'ERROR', 'message': f'There was an error when trying to obtain the report of departments that exceeded the average number of hires in {year} across all departments.'}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

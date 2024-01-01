from flask import Flask, jsonify
import logging
from controller import df_batch_insert_snowflake, execute_statement_snowflake, get_csv_from_bucket
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



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')

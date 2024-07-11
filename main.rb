require 'logger'
require 'cronitor'

import json
import re
from logging.handlers import RotatingFileHandler
import pandas

import requests
from dotenv import load_dotenv
import cronitor
import mysql.connector
from requests.auth import HTTPBasicAuth

log_file_path = Env['APP_PATH']
logger = Loger.new('import_subscribers.log', 10, 1024000)
#log_file_path = os.path.join(script_dir, 'import_subscribers.log')

load_dotenv()
script_dir = os.environ.get('SCRIPT_OUTPUT_DIR')

csv_file_path = os.path.join(script_dir, 'subscribers.csv')

cronitor.api_key = os.environ.get('CRONITOR_API_KEY')
cronitor_job_key = os.environ.get('CRONITOR_JOB_KEY')

lm_username = os.environ.get('LM_USERNAME') 
lm_password = os.environ.get('LM_PASSWORD')
lm_port = 9000
lm_host = "127.0.0.1"
lm_csv_params = {
        "mode": "subscribe",
        "delim": ",",
        "lists":[1],
        "overwrite": True
    }
                            
auth = HTTPBasicAuth(lm_username, lm_password)
headers = {'Content-Type': 'application/json'}

api_url_post = f"http://{lm_host}:{lm_port}/api/import/subscribers"

def is_valid_email(email):
    regex = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return bool(re.match(regex, email))

def import_csv():
    try:
        with open(csv_file_path, 'rb') as f:
            files = {'file': (f.name, f.read())}

        lm_csv_params_string = json.dumps(lm_csv_params)
        response = requests.post(api_url_post, auth=auth, data={"params": lm_csv_params_string}, files=files)
        response.raise_for_status()
    
    except Exception as e:
        logger.error('Exception error when processing SQL data: #{e}')
       raise e

def get_subscribers_into_csv(results, new_subscribers):
    try:
        email_sql = ""
        name_sql = ""
        role_sql = ""
        send_emails_sql = True
        status_sql = ""
        for row in results:
            name_sql = row[1]
            email_sql = row[2].lower()
            if row[3]:
                send_emails_sql = True
            else:
                send_emails_sql = False
            role_sql = row[4]
            status_sql = row[5]
            if is_valid_email(email_sql):
                status_attrib = f'{{"status": "{status_sql}", "role": "{role_sql}", "send_emails": "{send_emails_sql}"}}'
                new_subscribers.append([email_sql, name_sql, status_attrib])
        return new_subscribers

    except Exception as e:
        logger.error('Exception error when reading from sql and creating cvs: #{e}')
        raise e

def conditional_cronitor_job(func):
    if os.getenv('DEBUG') == 'true':
        return func
    else:
        return cronitor.job(cronitor_job_key)(func)

@conditional_cronitor_job
def main():
    logger.info('---------------------------------------')
    logger.info('Starts importing.')

    db_host = os.environ.get('SQL_HOSTNAME')
    db_name = os.environ.get('SQL_DBNAME')
    db_user = os.environ.get('SQL_USER')
    db_password = os.environ.get('SQL_PWD')

    connection = mysql.connector.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password
    )

    cursor = connection.cursor()
    offset = 0
    fetch_size = 500
    new_subscribers = []
    subscription_status = "ACTIVE"

    while True:
        """ sql_query = f"SELECT u.id, u.name, u.username, u.send_emails, r.description, s.status AS subscription_status FROM user u LEFT JOIN subscription s ON s.company_id = u.company_id LEFT OUTER JOIN role r ON r.id = u.role_id limit {fetch_size} OFFSET {offset}" """
        sql_query = f"SELECT u.id, u.name, u.username, u.send_emails, r.description, s.status AS subscription_status FROM user u LEFT JOIN subscription s ON s.company_id = u.company_id LEFT OUTER JOIN role r ON r.id = u.role_id where u.role_id = 2 and u.send_emails = true and s.status = '{subscription_status}' limit {fetch_size} OFFSET {offset}"
        cursor.execute(sql_query)
        results = cursor.fetchall()
        if not results:
            break

        subscribers_list = get_subscribers_into_csv(results, new_subscribers)
        offset += fetch_size

    csv_file_data_frame = pandas.DataFrame(subscribers_list, columns=['email','name','attributes'])
    csv_file_data_frame.to_csv(csv_file_path, index=False, encoding='utf-8')

    import_csv()

    if cursor:
        cursor.close()
    if connection:
        connection.close()
    
    try:
        os.remove(csv_file_path)
    except FileNotFoundError:
        logger.error('Subscribers CSV file not found. Skipping deletion.')

    logging.info('Import finished.')

if __name__ == "__main__":
    main()
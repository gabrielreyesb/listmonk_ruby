import os
import logging
from logging.handlers import RotatingFileHandler
from requests.auth import HTTPBasicAuth

import time
import requests
from dotenv import load_dotenv

load_dotenv()

script_dir = os.environ.get('SCRIPT_OUTPUT_DIR')
log_file_path = os.path.join(script_dir, 'import_subscribers.log')
max_bytes = 50 * 1024 * 1024
backup_count = 2
logging.basicConfig(filename=log_file_path,level=logging.INFO)
logging.getLogger('').handlers = []
handler = RotatingFileHandler(log_file_path, maxBytes=max_bytes, backupCount=backup_count)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger('').addHandler(handler)

lm_username = os.environ.get('LM_USERNAME')
lm_password = os.environ.get('LM_PASSWORD')

auth = HTTPBasicAuth(lm_username, lm_password)
headers = {'Content-Type': 'application/json'}

lm_port = 9000
lm_host = "127.0.0.1"
lm_subscribers_to_read=1000

api_url_get = f"http://{lm_host}:{lm_port}/api/subscribers?per_page={lm_subscribers_to_read}"
api_url_post = f"http://{lm_host}:{lm_port}/api/subscribers"
api_url_delete = f"{api_url_post}/"
wait_time = 1

def delete_subscribers():
    try:
        subscriber_ids = get_subscriber_ids()
        if not subscriber_ids:
            logging.info("No subscribers found to delete.")
            return
        
        total_deleted = 0
        total_per_cycle_deleted = 0
        for subscriber_id in subscriber_ids:
            if subscriber_id is None:
                break
            delete_url = f"{api_url_delete}{subscriber_id}"
            response_delete = requests.delete(delete_url, headers=headers, auth=auth)
            total_deleted += 1
            total_per_cycle_deleted += 1
        
        logging.info(f"{total_deleted} subscribers deleted.")
        time.sleep(wait_time)

    except Exception as e:
       logging.error(f"Exception error when processing data: '{e}'")
       raise e

def get_subscriber_ids():
  try:
    subscriber_ids = []
    page = 1
    while True:
        url = f"{api_url_get}&page={page}"
        response = requests.get(url, headers=headers, auth=auth)
        response.raise_for_status()
        data = response.json()
        if not data['data']['results']:
            break

        for subscriber in data['data']['results']:
            subscriber_id = subscriber.get('id')
            if subscriber_id:
                subscriber_ids.append(subscriber_id)
        page += 1

    return subscriber_ids
    
  except Exception as e:
    logging.error(f"Error retrieving subscriber IDs: {e}")
    return []
  
  
def main():
    logging.info('---------------------------------------')
    logging.info('Starts deleting.')

    delete_subscribers()
    
    logging.info('Deleting finished.')
    logging.getLogger('').handlers[0].flush()

if __name__ == "__main__":
    main()
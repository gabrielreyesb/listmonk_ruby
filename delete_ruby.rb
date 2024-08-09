require 'logger'
require 'rest-client'
require 'json'
require 'base64'

log_file_path = ENV['APP_PATH'] + '/my_app.log'
logger = Logger.new(log_file_path, 10, 10240000)
logger.formatter = proc do |severity, datetime, _progname, msg|
    "#{datetime.strftime("%Y-%m-%dT%H:%M:%S.%6N %z")} - #{severity} - #{msg}\n"
end

lm_username = ENV['LM_USERNAME']
lm_password = ENV['LM_PASSWORD']

lm_host = "127.0.0.1"
lm_port = 9000
lm_subscribers_to_read = 1000
api_url_get = "http://#{lm_host}:#{lm_port}/api/subscribers?per_page=#{lm_subscribers_to_read}"
api_url_delete = "http://#{lm_host}:#{lm_port}/api/subscribers/"
wait_time = 1

headers = { 'Content-Type' => 'application/json' }
auth = "Basic #{Base64.strict_encode64("#{lm_username}:#{lm_password}")}"

def delete_subscribers(logger, api_url_get, api_url_delete, auth, headers, wait_time)
    subscriber_ids = get_subscriber_ids(logger, api_url_get, auth, headers)
    return logger.info("No subscribers found to delete.") if subscriber_ids.empty?

    total_deleted = 0
    subscriber_ids.each do |subscriber_id|
        delete_url = "#{api_url_delete}#{subscriber_id}"
        response = RestClient.delete(delete_url, { authorization: auth, headers: headers })
        total_deleted += 1
        sleep(wait_time) 
    end
    logger.info("#{total_deleted} subscribers deleted.")
rescue RestClient::ExceptionWithResponse => e
    logger.error("Listmonk API error: #{e.http_code} - #{e.response}")
rescue StandardError => e
    logger.error("Error deleting subscribers: #{e.message}")
end

def get_subscriber_ids(logger, api_url_get, auth, headers)
    subscriber_ids = []
    page = 1
    begin 
        while true do
            url = "#{api_url_get}&page=#{page}"
            response = RestClient.get(url, { authorization: auth, headers: headers })
            data = JSON.parse(response.body)
            break if data.nil? || data['data'].nil? || data['data']['results'].nil?
            break if data['data']['results'].empty?  

            data['data']['results'].each do |subscriber|
                subscriber_ids << subscriber['id']
            end
            
            page += 1
        end
    end

    logger.info("Retrieved #{subscriber_ids.count} subscriber IDs.")
    subscriber_ids
rescue RestClient::ExceptionWithResponse => e
    logger.error("Listmonk API error: #{e.http_code} - #{e.response}")
    []
end

logger.info("---------------------------------------")
logger.info("Starts deleting.")

delete_subscribers(logger, api_url_get, api_url_delete, auth, headers, wait_time)

logger.info("Deleting finished.")
logger.close
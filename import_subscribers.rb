require 'logger'
require 'cronitor'
require 'net/http'
require 'uri'
require 'mysql2'
require 'csv'
require 'rest-client'
require 'base64'

logger = Logger.new('my_app.log', 10, 10240000)
app_path = ENV['APP_PATH']
cronitor_api_key = ENV['CRONITOR_API_KEY']
cronitor_job_key = ENV['CRONITOR_JOB_KEY']
lm_username = ENV['LM_USERNAME']
lm_password = ENV['LM_PASSWORD']
lm_port = 9000
lm_host = "127.0.0.1"
uri = URI("http://#{lm_host}:#{lm_port}/api/import/subscribers")
api_url_post = "http://#{lm_host}:#{lm_port}/api/import/subscribers"
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.request_uri, 'Content-Type' => 'application/json')
request.basic_auth(lm_username, lm_password)
new_subscribers = []
subscription_status = "ACTIVE"
sql_hostname = ENV['SQL_HOSTNAME']
sql_dbname = ENV['SQL_DBNAME']
sql_user = ENV['SQL_USER']
sql_pwd = ENV['SQL_PWD']
csv_file_path = app_path + "/subscribers.csv"
lm_csv_params = {
    "mode": "subscribe",
    "delim": ",",
    "lists": [1],
    "overwrite": true
}

def is_valid_email?(email)
  URI::MailTo::EMAIL_REGEXP.match?(email)
end

def get_subscribers_into_csv(results, new_subscribers, logger)
  new_subscribers = []
  results.each do |row|
    name_sql = row[:name]
    email_sql = row[:username]
    send_emails_sql = row[:send_emails] == 1
    role_sql = row[:description]
    status_sql = row[:status]
    if is_valid_email?(email_sql)
      status_attrib = { "status" => status_sql, "role" => role_sql, "send_emails" => send_emails_sql }.to_json
      new_subscribers << [email_sql, name_sql, status_attrib]
    end
  end
  new_subscribers
end

def write_subscribers_to_csv(subscribers_list, csv_file_path)
  CSV.open(csv_file_path, "wb", encoding: 'bom|utf-8') do |csv|
    csv << ["email", "name", "attributes"]
    subscribers_list.each do |subscriber|
      csv << subscriber
    end
  end
end

def import_csv(csv_file_path, logger, lm_csv_params, api_url_post, lm_username, lm_password)
  begin
    file_contents = File.read(csv_file_path, mode: 'rb')
    payload = {
      'params' => lm_csv_params.to_json,
      'file' => File.new(csv_file_path, 'rb')
    }
    response = RestClient.post(
      api_url_post,
      payload, { authorization: "Basic #{Base64.strict_encode64("#{lm_username}:#{lm_password}")}",}
    )
  rescue RestClient::ExceptionWithResponse => e
    logger.error("Listmonk API error: #{e.response.code} - #{e.response.body}")
    raise e
  rescue StandardError => e
    logger.error("Error during CSV import: #{e.message}")
    raise e
  end
end

Cronitor.configure do |config|
    config.api_key = cronitor_api_key
end

Cronitor.job(cronitor_job_key) do
    logger.info("---------------------------------------")
    logger.info("Import started.")

    offset = 0
    batch_size = 500

    db_client = Mysql2::Client.new(
        host: sql_hostname,
        database: sql_dbname,
        username: sql_user,
        password: sql_pwd,
    )

    begin
      loop do
        query = "SELECT u.id, u.name, u.username, u.send_emails, r.description, s.status FROM handy.user u LEFT JOIN handy.subscription s ON s.company_id = u.company_id LEFT OUTER JOIN handy.role r ON r.id = u.role_id where u.role_id = 2 and u.send_emails = true and s.status = '#{subscription_status}' LIMIT #{batch_size} OFFSET #{offset}"
        results = db_client.query(query, symbolize_keys: true)
        break if results.count == 0
        new_subscribers = get_subscribers_into_csv(results, new_subscribers, logger)
        write_subscribers_to_csv(new_subscribers, csv_file_path)
        new_subscribers.clear()
        offset += batch_size
      end
      import_csv(csv_file_path, logger, lm_csv_params, api_url_post, lm_username, lm_password)
    rescue Mysql2::Error => e
      puts "Error connecting to database: #{e.message}"
    ensure
      db_client&.close
    end
    logger.info("Import finished.")
end
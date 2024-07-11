require 'logger'
require 'cronitor'
require 'net/http'
require 'uri'
require 'mysql2'
require 'csv'

logger = Logger.new('my_app.log', 10, 10240000)
app_path = ENV['APP_PATH']
cronitor_api_key = ENV['CRONITOR_API_KEY']
cronitor_job_key = ENV['CRONITOR_JOB_KEY']
lm_username = ENV['LM_USERNAME']
lm_password = ENV['LM_PASSWORD']
lm_port = 9000
lm_host = "127.0.0.1"
uri = URI("http://#{lm_host}:#{lm_port}/api/import/subscribers")
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.request_uri, 'Content-Type' => 'application/json')
request.basic_auth(lm_username, lm_password)
#lm_csv_params = {"mode": "subscribe", "delim": ",", "lists":[1],"overwrite": True}

def is_valid_email?(email)
  URI::MailTo::EMAIL_REGEXP.match?(email)
end

def get_subscribers_into_csv(results, new_subscribers)
  new_subscribers = []
  results.each do |row|
    name_sql = row['name']
    email_sql = row['username']
    send_emails_sql = row['send_emails'] == 1
    role_sql = row['description']
    status_sql = row['status']

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

""" Cronitor.configure do |config|
    config.api_key = cronitor_api_key
end """

""" Cronitor.job(cronitor_job_key) do """
    logger.info("---------------------------------------")
    logger.info("Starts importing.")

    begin
      offset = 0
      batch_size = 500
      new_subscribers = []
      subscription_status = "ACTIVE"

      sql_hostname = ENV['SQL_HOSTNAME']
      sql_dbname = ENV['SQL_DBNAME']
      sql_user = ENV['SQL_USER']
      sql_pwd = ENV['SQL_PWD']

      csv_file_path = app_path + "/subscribers.csv"
      
      db_client = Mysql2::Client.new(
        host: sql_hostname,
        database: sql_dbname,
        username: sql_user,
        password: sql_pwd,
      )

      loop do
        query = "SELECT u.id, u.name, u.username, u.send_emails, r.description, s.status AS subscription_status FROM handy.user u LEFT JOIN handy.subscription s ON s.company_id = u.company_id LEFT OUTER JOIN handy.role r ON r.id = u.role_id where u.role_id = 2 and u.send_emails = true and s.status = '#{subscription_status}' LIMIT #{batch_size} OFFSET #{offset}"
        results = db_client.query(query symbolize_keys: true)

        break if results.count == 0

        new_subscribers = get_subscribers_into_csv(results, new_subscribers) 
        write_subscribers_to_csv(new_subscribers, csv_file_path) 
        new_subscribers.clear()
        
        offset += batch_size
      end

    
    
    rescue Mysql2::Error => e
      puts "Error connecting to database: #{e.message}"
    ensure
      db_client&.close
    end

    logger.info("Importing finished.")
""" end """
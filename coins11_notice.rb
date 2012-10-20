#coding: utf-8
require 'bundler/setup'
require 'net/https'
require 'oauth'
require 'json'
require 'yaml'
require 'thread'
require 'sqlite3'
require 'date'

class CoinsNoticeBot
  TWITTER_DATETIME_FORMAT = '%a %b %d %H:%M:%S +0000 %Y'
  DB_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
  CREDENTIAL_FILE = 'credential.yaml'
  HTTPS_CA_FILE = 'certificate.crt'
  SCREEN_NAME = 'coins11_notice'
  BOT_USER_AGENT = 'Announce bot for coins11 1.0 by @maytheplic'
  POST_DATABASE_FILE = 'posts.db'
  MAX_RETRY_COUNT = 10
  RETRY_INTERVAL = 30
  MY_ID = 552375459

  def initialize
    puts 'initializing...'
    @post_queue = Queue.new
    @message_recv_queue = Queue.new
    @message_send_queue = Queue.new
    
    open(CREDENTIAL_FILE) do |file|
      @credential = YAML.load(file)
    end
    
    @consumer = OAuth::Consumer.new(
      @credential['consumer_key'],
      @credential['consumer_secret']
    )

    @access_token = OAuth::AccessToken.new(
      @consumer,
      @credential['access_token'],
      @credential['access_token_secret']
    )
    puts 'initialized!'
  end

  def connect
    puts 'connecting to Twitter...'
    uri = URI.parse("https://userstream.twitter.com/1.1/user.json?track=#{SCREEN_NAME}")

    https = Net::HTTP.new(uri.host, uri.port)
    https.use_ssl = true
    https.ca_file = HTTPS_CA_FILE
    https.verify_mode = OpenSSL::SSL::VERIFY_PEER
    https.verify_depth = 5

    https.start do |https|
      request = Net::HTTP::Get.new(uri.request_uri)
      request["User-Agent"] = BOT_USER_AGENT
      request.oauth!(https, @consumer, @access_token)

      buf = ""
      https.request(request) do |response|
        response.read_body do |chunk|
          buf << chunk
          while ((line = buf[/.+?(\r\n)+/m]) != nil)
            begin
              buf.sub!(line, "")
              line.strip!
              status = JSON.parse(line)
            rescue
              break
            end

            yield status
          end
        end
      end
    end
  end

  def message_proc(json)
    puts 'processing message'
    sender = json['sender']
    text = json['text'].strip
    return if sender['id'] == MY_ID
    @post_queue.push text
    @message_send_queue.push Hash[:text, 'your post is accepted. thanks!', :user, sender['id']]
    datetime = DateTime.parse(json['created_at'], TWITTER_DATETIME_FORMAT)
    db = nil
    begin
      db = SQLite3::Database.new(POST_DATABASE_FILE)
    rescue
      puts 'error on opening db'
    end
    if db
      db.execute('INSERT INTO message VALUES(?, ?, ?, ?, ?)', json['id'], sender['id'], sender['screen_name'], datetime.strftime(DB_DATETIME_FORMAT), text)
    end
  end
  
  def post(status)
    puts 'posting status'
    @access_token.post('https://api.twitter.com/1/statuses/update.json',
                       'status' => status)
  end
  
  def send_direct_message(text, recipient_id)
    puts "send message to #{recipient_id}"
    @access_token.post('https://api.twitter.com/1/direct_messages/new.json',
                       'user_id' => recipient_id,
                       'text' => text)
  end

  def run
    puts 'bot is starting...'
    retry_count = 0
    receive_thread = Thread.new do
      puts 'receiver thread is ready.'
      begin
        loop do
          begin
            connect do |json|
              if json['direct_message']
                if json['direct_message']['sender']['id'] != MY_ID
                  @message_recv_queue.push json
                end
              end
            end
          rescue Timeout::Error, StandardError
            puts 'Connection to Twitter is disconnected or Application error was occured.'
            puts $!
            if (retry_count < MAX_RETRY_COUNT)
              retry_count += 1
              sleep RETRY_INTERVAL
              puts "Retry (count = #{retry_count})"
            else
              puts 'Reached retry limit.'
              abort
            end #endif
          end #end begin-rescue
        end #end loop
      end #end begin
    end #end receive_thread
   
    sleep 1

    message_recv_thread = Thread.new do
      puts 'message receive thread is ready.'
      loop do
        json = @message_recv_queue.pop
        message_proc json['direct_message']
      end
    end

    sleep 1
    
    message_send_thread = Thread.new do
      puts 'message send thread is ready.'
      loop do
        message = @message_send_queue.pop
        send_direct_message(message[:text], message[:user])
      end
    end

    sleep 1
    
    post_thread = Thread.new do
      puts 'post thread is ready.'
      loop do
        status = @post_queue.pop
        post status
      end
    end

    sleep 1

    puts 'bot is started!'
  end #end run method
end #end CoinsNotice class

bot = CoinsNoticeBot.new
bot.run
loop do
  sleep 1
end


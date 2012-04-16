#coding: utf-8
require 'net/https'
require 'oauth'
require 'json'
require 'psych'
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

  def initialize
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
  end

  def connect
    uri = URI.parse("https://userstream.twitter.com/2/user.json?track=#{SCREEN_NAME}")

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
    sender = json['sender']
    text = json['text'].strip
    @post_queue.push text
    @message_send_queue.push Hash[:text, 'your post is accepted. thanks!', :user, sender['id']]
  end
  
  def post(status)
    @access_token.post('https://api.twitter.com/1/statuses/update.json',
                       'status' => status)
  end
  
  def send_direct_message(text, recipient_id)
    @access_token.post('https://api.twitter.com/1/direct_messages/new.json',
                       'user_id' => recipient_id,
                       'text' => text)
  end

  def run
    retry_count = 0
    receive_thread = Thread.new do
      begin
        loop do
          begin
            connect do |json|
              if json['direct_message']
                @message_recv_queue.push json
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
    
    message_recv_thread = Thread.new do
      loop do
        json = @message_recv_queue.pop
        message_proc json['direct_message']
      end
    end
    
    message_send_thread = Thread.new do
      loop do
        message = @message_send_queue.pop
        send_direct_message(message[:text], message[:user])
      end
    end
    
    post_thread = Thread.new do
      loop do
        status = @post_queue.pop
        post status
      end
    end
  end #end run method
end #end CoinsNotice class

if __FILE__
  bot = CoinsNoticeBot.new
  bot.run
  loop do
    gets
  end
end


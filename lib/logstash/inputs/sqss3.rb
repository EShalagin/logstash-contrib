require "logstash/inputs/threadable"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require 'json'
require "tmpdir"

# Pull events from an Amazon Web Services Simple Queue Service (SQS) queue.
#
# SQS is a simple, scalable queue system that is part of the 
# Amazon Web Services suite of tools.
#
# Although SQS is similar to other queuing systems like AMQP, it
# uses a custom API and requires that you have an AWS account.
# See http://aws.amazon.com/sqs/ for more details on how SQS works,
# what the pricing schedule looks like and how to setup a queue.
#
# To use this plugin, you *must*:
#
#  * Have an AWS account
#  * Setup an SQS queue
#  * Create an identify that has access to consume messages from the queue.
#
# The "consumer" identity must have the following permissions on the queue:
#
#  * sqs:ChangeMessageVisibility
#  * sqs:ChangeMessageVisibilityBatch
#  * sqs:DeleteMessage
#  * sqs:DeleteMessageBatch
#  * sqs:GetQueueAttributes
#  * sqs:GetQueueUrl
#  * sqs:ListQueues
#  * sqs:ReceiveMessage
#
# Typically, you should setup an IAM policy, create a user and apply the IAM policy to the user.
# A sample policy is as follows:
#
#     {
#       "Statement": [
#         {
#           "Action": [
#             "sqs:ChangeMessageVisibility",
#             "sqs:ChangeMessageVisibilityBatch",
#             "sqs:GetQueueAttributes",
#             "sqs:GetQueueUrl",
#             "sqs:ListQueues",
#             "sqs:SendMessage",
#             "sqs:SendMessageBatch"
#           ],
#           "Effect": "Allow",
#           "Resource": [
#             "arn:aws:sqs:us-east-1:123456789012:Logstash"
#           ]
#         }
#       ]
#     } 
#
# See http://aws.amazon.com/iam/ for more details on setting up AWS identities.
#
class LogStash::Inputs::SQSS3 < LogStash::Inputs::Threadable
  include LogStash::PluginMixins::AwsConfig

  config_name "sqss3"
  milestone 1

  # Name of the SQS Queue name to pull messages from. Note that this is just the name of the queue, not the URL or ARN.
  config :queue, :validate => :string, :required => true
  
  # The number of seconds the service should wait for a response when requesting a new message
  config :wait_time_seconds , :validate => :number, :default => 20
  
  # Visibility timeout for messages that have been read from queue.
  # Value is in seconds.
  config :visibility_timeout, :validate => :number, :default => 1800

  public
  def aws_service_endpoint(region)
    return {
        :sqs_endpoint => "sqs.#{region}.amazonaws.com"
    }
  end

  public
  def register
    @logger.info("Registering SQS input", :queue => @queue)
    require "aws-sdk"

    @sqs = AWS::SQS.new(aws_options_hash)
	  @s3 = AWS::S3.new(aws_options_hash)

    begin
      @logger.debug("Connecting to AWS SQS queue", :queue => @queue)
      @sqs_queue = @sqs.queues.named(@queue)
      @logger.info("Connected to AWS SQS queue successfully.", :queue => @queue)  
    rescue Exception => e
      @logger.error("Unable to access SQS queue.", :error => e.to_s, :queue => @queue)
      throw e
    end # begin/rescue
  end # def register

  public
  def run(output_queue)
    @logger.debug("Polling SQS queue", :queue => @queue)


    receive_opts = {
      :limit => 1,
      :visibility_timeout => @visibility_timeout
    }

	

    continue_polling = true
	  message_received = true
    while running? && continue_polling && message_received
      message_received = false
      continue_polling = run_with_backoff(30, 1) do
        message=@sqs_queue.receive_message(receive_opts)
      end # run_with_backoff
      if message
        message_received = true
        begin
          jsonMessage=JSON.parse(message.body)
          puts jsonMessage
          record=jsonMessage["Records"][0]
          puts record
          bucket_description=record["s3"]["bucket"]
          puts bucket_description
          s3bucket = @s3.buckets[bucket_description["name"]]
          s3bucket.objects.with_prefix(record["s3"]["object"]["key"]).each(:limit => 1) do |object|
            process_log(output_queue, object)
          end #    process each objects
          message.delete
        rescue  Exception => bang
          @logger.error("Error reading SQS queue.", :error => bang, :queue => @queue)
        end
      end # valid SQS message
    end # polling loop
  end # def run

  def teardown
    @sqs_queue = nil
    finished
  end # def teardown

  private
  # Runs an AWS request inside a Ruby block with an exponential backoff in case
  # we exceed the allowed AWS RequestLimit.
  #
  # @param [Integer] max_time maximum amount of time to sleep before giving up.
  # @param [Integer] sleep_time the initial amount of time to sleep before retrying.
  # @param [Block] block Ruby code block to execute.
  def run_with_backoff(max_time, sleep_time, &block)
    if sleep_time > max_time
      @logger.error("AWS::EC2::Errors::RequestLimitExceeded ... failed.", :queue => @queue)
      return false
    end # retry limit exceeded

    begin
      block.call
    rescue AWS::EC2::Errors::RequestLimitExceeded
      @logger.info("AWS::EC2::Errors::RequestLimitExceeded ... retrying SQS request", :queue => @queue, :sleep_time => sleep_time)
      sleep sleep_time
      run_with_backoff(max_time, sleep_time * 2, &block)
    rescue AWS::EC2::Errors::InstanceLimitExceeded
      @logger.warn("AWS::EC2::Errors::InstanceLimitExceeded ... aborting SQS message retreival.")
      return false
    rescue Exception => bang
      @logger.error("Error reading SQS queue.", :error => bang, :queue => @queue)
      return false
    end # begin/rescue
    return true
  end # def run_with_backoff
  
  private
  def process_log(queue, object)

    @tmp = Dir.mktmpdir("logstash-")
    begin
      filename = File.join(@tmp, File.basename(object.key))
      File.open(filename, 'wb') do |s3file|
        object.read do |chunk|
          s3file.write(chunk)
        end
      end
      process_local_log(queue, filename)
      if @delete
        object.delete()
      end
    end
    FileUtils.remove_entry_secure(@tmp, force=true)

  end # def process_log

  private
  def process_local_log(queue, filename)

    metadata = {
      :version => nil,
      :format => nil,
    }

    if filename.end_with?('.zip')
		logextension = File.join(@tmp, File.basename("*.log"))
		zipcommand = 'unzip -j %{zipfile} -d "%{tmp}"' % { :zipfile => filename, :tmp => @tmp } 
		system( zipcommand )
		Dir.glob(logextension) do |entry|
			File.open(entry) do |file|
				file.each do |line|
					metadata = process_line(queue, metadata, line)
				end
			end
		end
    else
		File.open(filename) do |file|
			file.each do |line|
				metadata = process_line(queue, metadata, line)
			end
		end
    end

  end # def process_local_log

  private
  def process_line(queue, metadata, line)

    if /#Version: .+/.match(line)
      junk, version = line.strip().split(/#Version: (.+)/)
      unless version.nil?
        metadata[:version] = version
      end
    elsif /#Fields: .+/.match(line)
      junk, format = line.strip().split(/#Fields: (.+)/)
      unless format.nil?
        metadata[:format] = format
      end
    else
      @codec.decode(line) do |event|
        decorate(event)
        unless metadata[:version].nil?
          event["cloudfront_version"] = metadata[:version]
        end
        unless metadata[:format].nil?
          event["cloudfront_fields"] = metadata[:format]
        end
        queue << event
      end
    end
    return metadata

  end # def process_line
  
end # class LogStash::Inputs::SQS


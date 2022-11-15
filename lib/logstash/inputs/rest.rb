# encoding: utf-8
require "logstash/inputs/base"
require "stud/interval"
require "socket" # for Socket.gethostname
require "json"
require "rest-client"

# this require_relative returns early unless the JRuby version is between 9.2.0.0 and 9.2.8.0
require_relative "tzinfo_jruby_patch"

# Read from a URL and return JSON
#
# ==== Example usage
# The config should look like this:
#
#       rest {
#           name     => "arbitrary_name"
#           urls     => "https://domain.org/heatlh"
#           timeout  => "10"
#           headers  => { "x-api-access": "some-token", "x-custom-header": "value" }
#           schedule => "0 * * * *"
#       }
#
# ==== Scheduling
#
# Input from this plugin can be scheduled to run periodically according to a specific
# schedule. This scheduling syntax is powered by https://github.com/jmettraux/rufus-scheduler[rufus-scheduler].
# The syntax is cron-like with some extensions specific to Rufus (e.g. timezone support ).
#
# Examples:
#
# |==========================================================
# | `* 5 * 1-3 *`               | will execute every minute of 5am every day of January through March.
# | `0 * * * *`                 | will execute on the 0th minute of every hour every day.
# | `0 6 * * * America/Chicago` | will execute at 6:00am (UTC/GMT -5) every day.
# |==========================================================
#
#
# Further documentation describing this syntax can be found https://github.com/jmettraux/rufus-scheduler#parsing-cronlines-and-time-strings[here].

class LogStash::Inputs::Rest < LogStash::Inputs::Base
  config_name "rest"

  default :codec, "json"

  # URL to call
  config :url, :validate => :string, :required => true

  # Custom name of the call for better categorization of data
  config :name, :validate => :string, :required => true

  # HTTP headers for the REST call
  config :headers, :validate => :hash, :default => {}

  # Timeout (in seconds) for the REST call
  config :timeout, :validate => :number, :default => 60

  # Schedule of when to periodically run statement, in Cron format
  # for example: "* * * * *" (execute query every minute, on the minute)
  #
  # There is no schedule by default. If no schedule is given, then the statement is run
  # exactly once.
  config :schedule, :validate => :string

  public
  def register
    @logger = self.logger
    require "rufus/scheduler"
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)
    @logger.info("Registering rest Input", :type => @type, :url => @url, :name => @name,
                 :schedule => @schedule)
  end # def register

  def run(queue)
    if @schedule
      @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
      @scheduler.cron @schedule do
        send_request(queue)
      end

      @scheduler.join
    else
      send_request(queue)
    end
  end # def run

  def stop
    @scheduler.shutdown(:wait) if @scheduler
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
  end

  private

  def send_request(queue)
    RestClient::Request.execute(method: :get, url: url, timeout: timeout, accept: 'json', headers: headers) { |response, request, result, &block|
      @codec.decode(response) do |event|
        event["meta_name"] = name
        event["meta_host"] = @host
        event["meta_url"] = url
        event["meta_success"] = true
        event["meta_responseCode"] = response.code

        case response.code
        when 200
          event["meta_success"] = true
        else
          event["meta_success"] = false
        end
        decorate(event)
        queue << event
      end
    }
  end

end # class LogStash::Inputs::Rest

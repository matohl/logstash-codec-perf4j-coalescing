# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/line"
require "logstash/namespace"

# Add any asciidoc formatted documentation here
class LogStash::Codecs::Perf4jCoalescing < LogStash::Codecs::Base

  # This example codec will append a string to the message field
  # of an event, either in the decoding or encoding methods
  #
  # This is only intended to be used as an example.
  #
  # input {
  #   stdin { codec => perfj-coalescing }
  # }
  #
  # or
  #
  # output {
  #   stdout { codec => perfj-coalescing }
  # }
  config_name "perf4j-coalescing"

  # Append a string to the message
  config :append, :validate => :string, :default => ', Hello World!'

  public
  def register
    @lines = LogStash::Codecs::Line.new
    @lines.charset = "UTF-8"
  end

  public
  def decode(data)
    #puts "data=" + data
    @lines.decode(data) do |line|

      message = line["message"]
      #puts "length=" + message.length.to_s
      if message.length == 0
        #puts "end of line found"
        #yield LogStash::Event.new( { "message" => @buffer.join(NL) } )

        if @buffer.length > 2

          # first get start and end time from first line
          times = get_start_end_times(@buffer[0])
          startTime = times[0]
          endTime   = times[1]


          # then scrap first and second line
          @buffer.delete_at(1)
          @buffer.delete_at(0)
          @buffer.compact

          @buffer.each { |line|  yield LogStash::Event.new( { "message" => startTime + " " + endTime + "    " +  line } ) }

        end


        buffer_reset
      else
        #puts "adding " + message
        buffer_add(message)
      end

      #puts "line=" + line["message"]
      #replace = { "message" => line["message"].to_s + @append }

      #replace = { "message" => "hoj" }


    end
  end # def decode

  public
  def encode(event)
    @on_event.call(event, event["message"].to_s + @append + NL)
  end # def encode

  def buffer_add(message)
    (@buffer ||= []) << message
  end

  def buffer_reset
    @buffer.clear
  end

  def get_start_end_times(line)
    #2001-02-03T04:05+01:00

    arr = line.split(" ")
    date1 = arr[2] + "T" + arr[3]
    date2 = arr[5] + "T" + arr[6]

    # offset from UTC
    offset_minutes = Time.now.getlocal.utc_offset / 60

    if offset_minutes == 0
      time_zone_suffix = "Z"
    else
      hours = offset_minutes / 60
      minutes = offset_minutes % 60
      time_zone_suffix = "+" + sprintf("%2.2d%2.2d", hours, minutes)
    end
    date1 << time_zone_suffix
    date2 << time_zone_suffix

    values = []
    values.push(date1)
    values.push(date2)

    values
  end

end # class LogStash::Codecs::Perf4jCoalescing

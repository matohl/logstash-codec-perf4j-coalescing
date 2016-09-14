# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/line"
require "logstash/namespace"

require "logstash/util/charset"
require "logstash/timestamp"
require "logstash/codecs/auto_flush"



module LogStash module Codecs class Perf4jCoalescing < LogStash::Codecs::Base
  config_name "perf4j-coalescing"

  # This coalescing perf4j codec will BLA BLA TODO
  #
  # This is only intended to be used as an example.
  #
  # input {
  #   stdin { codec => perfj-coalescing }
  # }

  # Logstash ships by default with a bunch of patterns, so you don't
  # necessarily need to define this yourself unless you are adding additional
  # patterns.
  #
  # Pattern files are plain text with format:
  # [source,ruby]
  #     NAME PATTERN
  #
  # For example:
  # [source,ruby]
  #     NUMBER \d+
  config :patterns_dir, :validate => :array, :default => []

  # The character encoding used in this input. Examples include `UTF-8`
  # and `cp1252`
  #
  # This setting is useful if your log files are in `Latin-1` (aka `cp1252`)
  # or in another character set other than `UTF-8`.
  #
  # This only affects "plain" format logs since JSON is `UTF-8` already.
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  # The accumulation of events can make logstash exit with an out of memory error
  # if event boundaries are not correctly defined. This settings make sure to flush
  # multiline events after reaching a number of lines, it is used in combination
  # max_bytes.
  #config :max_lines, :validate => :number, :default => 500
  config :max_lines, :validate => :number, :default => 5

  # The accumulation of events can make logstash exit with an out of memory error
  # if event boundaries are not correctly defined. This settings make sure to flush
  # multiline events after reaching a number of bytes, it is used in combination
  # max_lines.
  config :max_bytes, :validate => :bytes, :default => "10 MiB"

  # The accumulation of multiple lines will be converted to an event when either a
  # matching new line is seen or there has been no new data appended for this time
  # auto_flush_interval. No default.  If unset, no auto_flush. Units: seconds
  config :auto_flush_interval, :validate => :number

  public
  def register
    require "grok-pure" # rubygem 'jls-grok'
    require 'logstash/patterns/core'

    # The regular expression to match.
    @pattern=""

    # If the pattern matched, does event belong to the next or previous event?
    @what =  "previous"

    # Negate the regexp pattern ('if not matched').
    @negate = false

    @auto_flush_interval = 5


    # Detect if we are running from a jarfile, pick the right path.
    patterns_path = []
    patterns_path += [LogStash::Patterns::Core.path]

    @grok = Grok.new

    @patterns_dir = patterns_path.to_a + @patterns_dir
    @patterns_dir.each do |path|
      if ::File.directory?(path)
        path = ::File.join(path, "*")
      end

      Dir.glob(path).each do |file|
        @logger.info("Grok loading patterns from file", :path => file)
        @grok.add_patterns_from_file(file)
      end
    end

    @grok.compile(@pattern)
    @logger.debug("Registered Perf4jCoalescing plugin", :type => @type, :config => @config)

    reset_buffer

    @handler = method("do_#{@what}".to_sym)

    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
    if @auto_flush_interval
      # will start on first decode
      @auto_flush_runner = AutoFlush.new(self, @auto_flush_interval)
    end
  end # def register

  def use_mapper_auto_flush
    return unless auto_flush_active?
    @auto_flush_runner = AutoFlushUnset.new(nil, nil)
    @auto_flush_interval = @auto_flush_interval.to_f
  end

  def accept(listener)
    # memoize references to listener that holds upstream state
    @previous_listener = @last_seen_listener || listener
    @last_seen_listener = listener
    decode(listener.data) do |event|
      what_based_listener.process_event(event)
    end
  end

  def decode(text, &block)
    text = @converter.convert(text)
    text.split("\n").each do |line|
      match = @grok.match(line)
      @logger.debug("Perf4jCoalescing", :pattern => @pattern, :text => line,
                    :match => !match.nil?, :negate => @negate)

      # Add negate option
      #match = (match and !@negate) || (!match and @negate)

      if line.length == 0
        match = true
      else
        match = false
      end
      @handler.call(line, match, &block)
    end
  end # def decode

  def buffer(text)
    @buffer_bytes += text.bytesize
    @buffer.push(text)
  end

  def flush(&block)
    @logger.info("Flush called")
    if block_given? && @buffer.any?
      no_error = true
      events = merge_events
      begin
        #yield events
        events.each  { |evt|
          yield evt
        }
      rescue ::Exception => e
        # need to rescue everything
        # likliest cause: backpressure or timeout by exception
        # can't really do anything but leave the data in the buffer for next time if there is one
        @logger.error("Multiline: flush downstream error", :exception => e)
        no_error = false
      end
      reset_buffer if no_error
    end
  end

  def auto_flush(listener = @last_seen_listener)
    return if listener.nil?

    flush do |event|
      listener.process_event(event)
    end
  end

  def merge_events

    # when we end up here, there should be an entire block of coalesced performance statistics in the buffer,
    # where the first line will contain the time interval, the second is just headings (discard it), and from
    # the third line there are real statistics
    events = []
    if @buffer.length > 2

      # first get start and end time from first line
      times = get_start_end_times(@buffer[0])
      startTime = times[0]
      endTime   = times[1]


      # then scrap first and second line
      @buffer.delete_at(1)
      @buffer.delete_at(0)
      @buffer.compact

      @buffer.each { |line|

        splitLine = line.split(" ")

        events.push( LogStash::Event.new(
                LogStash::Event::TIMESTAMP => startTime,
                "message" => startTime + " " + endTime + "    " +  line,
                "startTime" => startTime,
                "endTime" => endTime,
                "tag" => splitLine[0],
                "average" => splitLine[1],
                "min" => splitLine[2],
                "max" => splitLine[3],
                "StdDev" => splitLine[4],
                "count" => splitLine[5]
            ))
      }

    end

    events
  end

  def reset_buffer
    @buffer = []
    @buffer_bytes = 0
  end

  def doing_previous?
    @what == "previous"
  end

  def what_based_listener
    doing_previous? ? @previous_listener : @last_seen_listener
  end
=begin
  def do_next(text, matched, &block)
    buffer(text)
    auto_flush_runner.start
    flush(&block) if !matched || buffer_over_limits?
  end
=end

  def do_previous(text, matched, &block)
    flush(&block) if matched #if !matched || buffer_over_limits?
    auto_flush_runner.start
    if !matched
      buffer(text)
      @logger.info("add to buffer:" + text)
    end
  end

  def over_maximum_lines?
    @buffer.size > @max_lines
  end

  def over_maximum_bytes?
    @buffer_bytes >= @max_bytes
  end

  def buffer_over_limits?
    over_maximum_lines? || over_maximum_bytes?
  end

  def encode(event)
    # Nothing to do.
    @on_event.call(event, event)
  end # def encode

  def close
    auto_flush_runner.stop
  end

  def auto_flush_active?
    !@auto_flush_interval.nil?
  end

  def auto_flush_runner
    @auto_flush_runner || AutoFlushUnset.new(nil, nil)
  end

  def get_start_end_times(line)
    #2001-02-03T04:05+01:00

    arr = line.split(" ")
    date1 = arr[2] + "T" + arr[3] + ".000"
    date2 = arr[5] + "T" + arr[6] + ".000"

    # offset from UTC
    offset_minutes = Time.now.getlocal.utc_offset / 60

    if offset_minutes == 0
      time_zone_suffix = "Z"
    else
      hours = offset_minutes / 60
      minutes = offset_minutes % 60
      time_zone_suffix = "+" + sprintf("%2.2d:%2.2d", hours, minutes)
    end
    date1 << time_zone_suffix
    date2 << time_zone_suffix

    values = []
    values.push(date1)
    values.push(date2)

    values
  end

end end end # class LogStash::Codecs::Perf4jCoalescing
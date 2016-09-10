# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/perf4j-coalescing"


describe LogStash::Codecs::Perf4jCoalescing do

  subject do
    LogStash::Codecs::Perf4jCoalescing.new
  end

=begin
  it "should append a string to data" do
    data = "Test\n"
    subject.decode(data) do |event|
      puts event["message"]
      expect(event["message"]).to eq("Test, Hello World!")
    end
  end
=end

  it "should parse perf4j block" do
    data = "Performance Statistics   2016-09-01 01:50:00 - 2016-09-01 01:55:00\nTag                                                  Avg(ms)         Min         Max     Std Dev       Count\nAppService-getMessages                                 127.8          48         254        84.4           4\n\n"
    subject.decode(data) do |event|
      puts event["message"]
      expect(true)
    end
  end

=begin
  it "should separate three blocks" do
    data = "one\n\ntwo\ntwo\n\nthree\nthree\nthree\n\n"
    subject.decode(data) do |event|
      puts event["message"]
      expect(true)
    end
    end

  it "should" do
    str = "Performance Statistics   2016-09-01 13:00:00 - 2016-09-01 13:05:00"
    #str.scan("\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d") {|match| puts match}
    #out = str.scan("\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d")
    #puts out
    #puts "len" + out.length.to_s

    arr = str.split(" ")
    arr.each { |item| puts item  + "\n" }

    date1 = arr[2] + " " + arr[3]
    date2 = arr[5] + " " + arr[6]

    puts "date1=" + date1
    puts "date2=" + date2

    puts "timezone is " + Time.now.getlocal.zone
    puts "timezone offset is " + Time.now.getlocal.utc_offset.to_s
    offset_minutes = Time.now.getlocal.utc_offset / 60
    puts "minutes offset is " + offset_minutes.to_s
    hours = offset_minutes / 60
    minutes = offset_minutes % 60
    puts "hours=" + hours.to_s + ", minutes=" + minutes.to_s

    iso8601_offset = sprintf("%2.2d%2.2d", hours, minutes)
    puts "offset=" + iso8601_offset





    expect(true)
  end
=end

end

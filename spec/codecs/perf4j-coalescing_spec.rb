# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/codecs/perf4j-coalescing"


describe LogStash::Codecs::Perf4jCoalescing do

  subject do
    LogStash::Codecs::Perf4jCoalescing.new
  end

  it "should append a string to data" do
    data = "Test\n"
    subject.decode(data) do |event|
      puts event["message"]
      expect(event["message"]).to eq("Test, Hello World!")
    end
  end
end

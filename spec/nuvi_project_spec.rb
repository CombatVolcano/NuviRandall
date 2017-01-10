require_relative '../NuviProject.rb'

RSpec.describe DataProcessor, "#application" do

	context "it resolves pages" do
		it "resolves a normal page" do
			d = DataProcessor.new("TEST_VALUES")
			resolved = d.checkResponse("http://www.google.com")
			expect(resolved).to eq [URI("http://www.google.com"), URI("http://www.google.com")]
		end
	end

	context "it uses redis" do
		it "connects to redis" do
			d = DataProcessor.new("TEST_VALUES")
			redis_name = d.redisInit("127.0.0.1", 9999)
			expect(redis_name.to_s).to start_with("#<Redis:")
		end

		it "writes test data to redis and then deletes it" do
			d = DataProcessor.new("TEST_VALUES")
			d.writeRedis("test_key", "test_value")
			expect(d.deleteRedisList("test_key", "test_value")).to eq 0
			expect(d.deleteRedisSet("test_key", "test_value")).to eq 0
		end
	end

end

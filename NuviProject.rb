#"Nuvi interview code project for Randall Feuerlein refeuerlein@gmail.com"
require 'open-uri'
require 'net/http'
require 'zip'
require 'redis'
require 'rexml/document'

class DataProcessor

	#select verbose output
	@@verbose = false

	def checkResponse(sourcePage)
		uri = URI(sourcePage)
		puts "Checking response for: " + sourcePage.to_s
		response = Net::HTTP.get_response(uri)
		puts "HTTP response: " + response.to_s
		case response
		when Net::HTTPOK then
			location = URI(sourcePage) 
			return uri, location
		when Net::HTTPRedirection
			puts "Redirected to: " + response['location'].to_s
			checkResponse(response['location'])
		else
			puts "HTTP Exception: " + response.to_s
			puts "Exiting..."
			exit 1
		end
	end

	def redisInit(redisHost, redisPort)
		redis = Redis.new(:host => redisHost, :port => redisPort)
		print "Connecting to redis... "
		begin
			redisTest = redis.ping
		rescue Redis::CannotConnectError => e
			puts " failed: "
			puts e.to_s
			exit 1
		end
		puts " success: " + redis.to_s
		return redis
	end

	def downloadFile(filename, localPath)
		filePath = @location + filename.to_s
		begin
			download = open(filePath)
		rescue OpenURI::HTTPError => e
			puts "HTTPError - Failed to download file: " + filePath.to_s
		end
		IO.copy_stream(download, localPath)
	end

	def downloadZipFiles()
		downloaded = 0
		@page.each_line do |line|
			filename = line.match(/\d+.zip/)
			if filename
				puts "Downloading file: " + filename.to_s
				localPath = @workingDirectory + filename.to_s
				downloadFile(filename, localPath)
				downloaded += 1
				if downloaded == @downloadLimit
		   			break
		   		end
			end
		end
	end

	def extractFiles()
		zipFiles = Dir.glob(@workingDirectory.to_s + "*.zip")
		zipFiles.each do |zfile|
			puts "Unpacking ZIP: " + zfile.to_s if not @@verbose
			logData("Unpacking ZIP: " + zfile.to_s)
			Zip::File.open(zfile) do |zipfile|
				zipfile.each do |file|
					logData("Extracting file: " + file.to_s)
					path = File.join(@workingDirectory, file.name)
					logData("Extract path: " + path.to_s)
					if not File.exist?(path)
						file.extract(path)
					else
						logData(path.to_s + " exists! Not extracted.")
					end
		   		end
		   	end
		end
	end

	def processDocuments()
		puts ("Processing XML documents...") if not @@verbose
		logData("Processing XML documents...")
		files = Dir.glob(@workingDirectory.to_s + "*.xml")
		files.each do |xmlFile|
			logData("Processing document: " + xmlFile.to_s + " - ")
			content = File.read(xmlFile)
			if content.lines.first.match(/^<\?xml/)
				logData("XML document type verified")
				doc = REXML::Document.new(content)
	   			doc.elements.each("document/discussion_title") { |element| logData(element.text) }
	   			writeRedis(@redisRepo, doc)
	   		else
	   			logData("is not a properly formatted XML document!")
	   		end
	   	end
	end

	def writeRedis(key, value)
		logData("checking for existence of key in set: ")
		if not @redis.sismember(key + "set", value)
			logData("value not found, writing...")
			@redis.sadd(key + "set", value) #add to set
			@redis.rpush(key, value) #add to list
		else
			logData("SET key exists, not writing")
		end
	end

	def deleteRedisList(key, value)
		if @redis.lrem(key, -1, value)
			return 0
		else
			return 1
		end
	end

	def deleteRedisSet(key, member)
		if not @redis.srem(key, member)
			return 0
		else
			return 1
		end
	end

	def getPage(uri)
		page = Net::HTTP.get(URI(uri))
		return page
	end

	def clearZips()
		files = Dir.glob(@workingDirectory.to_s + "*.zip")
		files.each do |filesToDelete|
			FileUtils.rm(filesToDelete)
		end
	end

	def logData(log)
		if log
			open(@logDirectory + "/log_file.txt", "a") do |log_file|
				log_file << Time.now.to_s + " : " + log + "\n"
			end
		end
		puts log if @@verbose
	end

	def processData
		logData("Download limit is: " + @downloadLimit.to_s)
		downloadZipFiles()
		extractFiles()
		processDocuments()
		puts "Nuvi interview code project for Randall Feuerlein refeuerlein@gmail.com"
		exit 0
	end

	def initialize(redis_repo)
		#configure working directory
		@workingDirectory = "/tmp/"
		puts "Working directory is: " + @workingDirectory

		#delete existing ZIP files
		puts "Deleting ZIP files in working directory..."
		clearZips()
		
		#log file location
		@logDirectory = File.expand_path(File.dirname(__FILE__)) #script location
		puts "Log directory is: " + @logDirectory

		#configure source website
		sourcePage = "http://bitly.com/nuvi-plz"
		puts "Source url is: " + sourcePage

		#configure redis hostname and port
		redisHost = "127.0.0.1"
		redisPort = 9999
		puts "Redis host is: " + redisHost.to_s + ":" + redisPort.to_s 

		#configure download limit
		@downloadLimit = 1
		puts "Download limit is: " + @downloadLimit.to_s

		@redisRepo = redis_repo

		#connect to redis
		@redis = redisInit(redisHost, redisPort)
		
		#resolve hostname
		@uri, @location = checkResponse(sourcePage)

		#get page data
		@page = getPage(@uri)
	end

end #DataProcessor

#comment out these lines to run tests
main = DataProcessor.new("NEWS_XML")
main.processData()
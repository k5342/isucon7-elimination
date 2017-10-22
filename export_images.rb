require 'mysql2'

client = Mysql2::Client.new(:host => "db", :username => "isucon", :database => "isubata", :password => 'isucon')

results = client.query("SELECT * FROM image")

results.each do |r|
  id = r["id"]
  data = r["data"]
  file_name = r["name"]

#  ext = case mime
#        when /\/jp/
#          "jpg"
#        when /\/pn/
#          "png"
#        when /\/gi/
#          "gif"
#        end

  File.open("exports/#{file_name}", "wb").write(data)
end

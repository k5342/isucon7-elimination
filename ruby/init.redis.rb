require 'redis'
require 'mysql2-cs-bind'

db = Mysql2::Client.new(
  host: ENV.fetch('ISUBATA_DB_HOST') { 'localhost' },
  port: ENV.fetch('ISUBATA_DB_PORT') { '3306' },
  username: ENV.fetch('ISUBATA_DB_USER') { 'isucon' },
  password: ENV.fetch('ISUBATA_DB_PASSWORD') { 'isucon' },
  database: 'isubata',
  encoding: 'utf8mb4'
)
db.query('SET SESSION sql_mode=\'TRADITIONAL,NO_AUTO_VALUE_ON_ZERO,ONLY_FULL_GROUP_BY\'')


# 現在入っているデータの削除
Thread.new {
  Redis.current.keys('isu7:*').each_slice(1000) { |keys| Redis.current.del(keys) }
}.join

puts "=> SELECT * FROM message"
db.xquery('SELECT * FROM message').each_slice(10000).with_index do |slice, idx|
    puts "==> messages:#{idx}"

    messages = []
    slice.each do |msg|
      id, channel_id, user_id, content, created_at = msg.values_at('id', 'channel_id', 'user_id', 'content', 'created_at')
      messages << { id: id, channel_id: channel_id, user_id: user_id, content: content, created_at: created_at }
    end

    #hoge = messages.group_by { |msg| msg[:channel_id] }
    #               .map { |k,arr| [k.to_s, {id: arr[:id], channel_id: arr[:channel_id] }] }
    #puts hoge

    messages.each do |hash|
      Redis.current.hmset(
        "isu7:message-#{hash[:id]}",
        :channel_id, hash[:channel_id],
        :user_id, hash[:user_id],
        :content, hash[:content],
        :created_at, hash[:created_at]
      )

      #Redis.current.set("isu7:channel_ids:#{hash[:channel_id]}", [])
      Redis.current.set("isu7:last_message_id", hash[:id])

      Redis.current.rpush("isu7:channel_ids:#{hash[:channel_id]}", hash[:id])
    end
    puts " * messages:#{idx}"
end

require 'mysql2'

def delete_rows_for_ratio(client, table_name, delete_ratio)
  base_count = client.query("select count(1) as count from #{table_name}").to_a.first["count"]
  puts base_count
  client.query("DELETE FROM #{table_name} LIMIT #{(base_count - (base_count / delete_ratio))};")
  new_count = client.query("select count(1) as count from #{table_name}").to_a.first["count"]
  puts new_count
end

def chain_relationship(client, table_chain, table_name)
  return if table_chain[table_name].nil?
  table_chain[table_name].each do |tbl|
    puts "==== start trim " + tbl["name".to_sym]
    old = client.query("select count(1) as count from #{tbl["name".to_sym]}").to_a.first["count"]
    client.query("DELETE FROM #{tbl["name".to_sym]} WHERE #{tbl["key".to_sym]} NOT IN (SELECT #{tbl["ref_key".to_sym]} FROM #{table_name});")
    puts "#{old} -> #{client.query("select count(1) as count from #{tbl["name".to_sym]}").to_a.first["count"]}"
    puts "===="
    chain_relationship(client, table_chain, tbl["name".to_sym])
  end
end

puts ENV['MYSQL_HOST']
client = Mysql2::Client.new(:host => ENV['MYSQL_HOST'], :username => 'root', :password => 'root', :encoding => 'utf8', :database => 'rubis')

table_chain = {
  "regions" => [{name: "users", key: "id", ref_key: "region"}],
  "users" => [{name: "items", key: "id", ref_key: "seller"}, {name: "buynow", key: "id", ref_key: "buyer_id"}, {name: "bids", key: "id", ref_key: "user_id"}],
  "categories" => [{name: "items", key: "id", ref_key: "category"}],
  "items" => [{name: "buynow", key: "id", ref_key: "iitem_id"}, {name: "bids", key: "id", ref_key: "item_id"}]
}


#delete_rows_for_ratio(client, "regions", 10)
#delete_rows_for_ratio(client, "categories", 10)
chain_relationship(client, table_chain, "regions")
chain_relationship(client, table_chain, "categories")

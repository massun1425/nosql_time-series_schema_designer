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
    if tbl.has_key? "composite".to_sym
      client.query("DELETE FROM #{tbl["name".to_sym]} WHERE (#{tbl["key".to_sym]}, #{tbl["composite".to_sym]}) NOT IN (SELECT #{tbl["ref_key".to_sym]},#{tbl["composite_ref_key".to_sym]} FROM #{table_name});")
    else
      client.query("DELETE FROM #{tbl["name".to_sym]} WHERE #{tbl["key".to_sym]} NOT IN (SELECT #{tbl["ref_key".to_sym]} FROM #{table_name});")
    end
    puts "#{old} -> #{client.query("select count(1) as count from #{tbl["name".to_sym]}").to_a.first["count"]}"
    puts "===="
    chain_relationship(client, table_chain, tbl["name".to_sym])
  end
end

puts ENV['MYSQL_HOST']
client = Mysql2::Client.new(:host => ENV['MYSQL_HOST'], :username => 'root', :password => 'root', :encoding => 'utf8', :database => 'tpch')

table_chain = {
  "region" => [{name: "nation", key: "n_regionkey", ref_key: "r_regionkey"}],
  "nation" => [{name: "supplier", key: "s_nationkey", ref_key: "n_nationkey"}, {name: "customer", key: "c_nationkey", ref_key: "n_nationkey"}],
  "supplier" => [{name: "partsupp", key: "ps_suppkey", ref_key: "s_suppkey"}],
  "customer" => [{name: "orders", key: "o_custkey", ref_key: "c_custkey"}],
  "orders" => [{name: "lineitem", key: "l_orderkey", ref_key: "o_orderkey"}],
  "part" => [{name: "partsupp", key: "ps_partkey", ref_key: "p_partkey"}],
  "partsupp" => [{name: "lineitem", key: "l_partkey", composite: "l_suppkey", ref_key: "ps_partkey", composite_ref_key: "ps_suppkey"}]
}


#delete_rows_for_ratio(client, "supplier", 100)
#delete_rows_for_ratio(client, "part", 100)
#delete_rows_for_ratio(client, "customer", 50)

delete_rows_for_ratio(client, "supplier", 10)
delete_rows_for_ratio(client, "part", 10)
delete_rows_for_ratio(client, "customer", 10)
chain_relationship(client, table_chain, "region")
chain_relationship(client, table_chain, "part")

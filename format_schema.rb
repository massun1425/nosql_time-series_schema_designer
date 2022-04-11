#require 'csv'
filename=ARGV[0]

is_schema = false
File.foreach(filename) do |line|
   is_schema = false if line =~ /<\/json format>/ 
   puts line if is_schema
   is_schema = true if line =~ /<json format>/ 
end

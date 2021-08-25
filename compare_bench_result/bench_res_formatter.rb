require 'csv'
filename=ARGV[0]

begin
  File.foreach(filename) do |line|
    begin
      parsed_line = CSV.parse(line)[0]
      if parsed_line.size == 10 and (parsed_line[0] =~ /^[0-9]+?$/ or parsed_line[0] == "timestep")
        puts line
      end
    rescue
    end
  end
rescue SystemCallError => e
  puts e
end

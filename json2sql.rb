####################################################
####################################################
####                                            ####
####           JSON to SQL Conversion           ####
####                                            ####
#### Mined Minds 2018                           ####
####                                            gb##
####################################################
####################################################

require_relative("functions.rb")

file_name = ARGV[0] || ""
if file_name.length == 0
  puts "Please enter a valid file name"
  puts "Example: ruby j2s.rb data_*.json"
  exit
end

file_list = wildcard_fopen(file_name)
if file_list.length == 0
  puts "Invalid file name: #{file_name}"
  exit
end

<<-HEREDOC
    json2sql Copyright (C) 2018 Gary Boyd
    This program comes with ABSOLUTELY NO WARRANTY; for details type `show w'.
    This is free software, and you are welcome to redistribute it
    under certain conditions; type `show c' for details.
HEREDOC

file_list.each do |file|
  file[:file].readlines.each_with_index do |json_data, index|
    t_tables, t_columns, t_entries = create_entries_from_json(JSON.parse(json_data))

    f_out = File.new(file[:file_name] + ".txt", 'w')
    create_table_queries(t_tables, t_columns).each do |l|
      f_out.write(l + ";\n")
    end
    f_out.write("\n")
    create_insert_queries(t_entries, t_tables, 1).each do |l|
      f_out.write(l + ";\n")
    end
  end
end
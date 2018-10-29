####################################################
####################################################
####                                            ####
####           JSON to SQL Conversion           ####
####                                            ####
#### Mined Minds 2018                           ####
####                                            gb##
####################################################
####################################################

# Json '2' SQL is a tool for converting raw json data to SQL format
# Copyright (C) 2018 Gary Boyd

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program. (See /COPYING)  If not, see <https://www.gnu.org/licenses/>.

require_relative("functions.rb")
require_relative("db_connect.rb")

$config_vars = {
  schema: "",
  text_out: true,
  db_out: false,
  db_host: "",
  db_pass: "",
  db_user: "",
  db_port: "",
  db_name: "",
  db_type: "azure",
  pre_columns: [],
  pre_columns_string: "",
  do_insert: true,
  do_tables: true
}

ARGV.each_with_index do |arg, i|
  case arg
  when '--help'
puts <<-HELPDOC
Use:
  json2sql [filename] [options]
      Accepts wildcards with *

Options:
  --help                    Displays this message
  --schema [name]           Adds a schema prefix to SQL commands

Output options:
  --out-text                Outputs the SQL to a text file (input_filename.txt)
  --out-db [type]           Will directly insert data into the database. Supported types: azure
    --db-host [host]
    --db-user [username]
    --db-pass [password]
    --db-port [port]
    --db-name [name]
  --p-cols                  Allows you to determine predefined columns in all tables. Seperate with comma, no spaces. Default: id[bigint]
  --insert-only             Only generates insert statements
  --table-only              Only generate create table statements

HELPDOC
    exit
  when '--schema'
    $config_vars[:schema] = ARGV[i+1]
  when '--out-text'
    $config_vars[:text_out] = true
  when '--out-db'
    $config_vars[:db_out] = true
    $config_vars[:db_type] = ARGV[i+1]
  when '--db-host'
    $config_vars[:db_host] = ARGV[i+1]
  when '--db-user'
    $config_vars[:db_user] = ARGV[i+1]
  when '--db-pass'
    $config_vars[:db_pass] = ARGV[i+1]
  when '--db-port'
    $config_vars[:db_port] = ARGV[i+1]
  when '--db-name'
    $config_vars[:db_name] = ARGV[i+1]
  when '--p-cols'
    $config_vars[:pre_columns_string] = ARGV[i+1]
  when '--insert-only'
    $config_vars[:do_insert] = true
    $config_vars[:do_tables] = false
  when '--table-only'
    $config_vars[:do_tables] = true
    $config_vars[:do_insert] = false
  end
end

if $config_vars[:db_out] == true
  hit_issue = false
  if $config_vars[:db_host] == ""
    hit_issue = true
    puts "Must provide a host when using db connection"
  end
  if $config_vars[:db_port] == ""
    hit_issue = true
    puts "Must provide a port when using db connection"
  end
  if $config_vars[:db_user] == ""
    hit_issue = true
    puts "Must provide a user when using db connection"
  end
  if $config_vars[:db_pass] == ""
    hit_issue = true
    puts "Must provide a password when using db connection"
  end
  if hit_issue == true
    puts "Please see json2sql --help for details"
    exit
  end
end

if $config_vars[:pre_columns_string] != ""
  r_hash = []
  $config_vars[:pre_columns_string].split(",").each do |col|
    col_name = col.split("[")[0]
    col_type = col.split("[")[1].chomp("]")
    r_hash << {name: col_name, type: col_type}
  end
  $config_vars[:pre_columns] = r_hash
end
p $config_vars

file_name = ARGV[0] || ""
if file_name.length == 0
  puts "Please enter a valid file name"
  puts "See json2sql --help"
  exit
end

file_list = wildcard_fopen(file_name)
if file_list.length == 0
  puts "Invalid file name: #{file_name}"
  puts "See json2sql --help"
  exit
end

system('cls') || system('clear')

puts <<~HEREDOC

    json2sql Copyright (C) 2018 Gary Boyd
    This program comes with ABSOLUTELY NO WARRANTY.
    This is free software, and you are welcome to redistribute it
    under certain conditions.

HEREDOC

puts "\n"

file_list.each do |file|
  file_tables = []
  file_columns = []
  file_entries = []

  cur_file = file[:file_name] + "." + file[:file_extension]
  file_lines = file[:file].count
  line_counter = 1
  file[:file].rewind

  file[:file].each do |json_data|
    print "Parsing entry: #{line_counter}/#{file_lines} [#{cur_file}]\r"
    t_tables, t_columns, t_entries = create_entries_from_json(JSON.parse(json_data), line_counter)

    #file_tables << "INSERT INTO raw_data (product_id,data) VALUES(1,#{json_data})"
    file_tables << t_tables
    file_columns << t_columns
    file_entries << t_entries

    line_counter += 1
    $stdout.flush
  end
  file[:file].close
p line_counter
  puts "File complete: #{cur_file}                     "

  if line_counter > 1
    f_out = File.new(file[:file_name] + ".txt", 'w')
      create_table_queries(file_tables, file_columns).each do |l|
      f_out.write(l + ";\n")
    end
    f_out.write("\n")
    create_insert_queries(file_entries, file_tables).each do |l|
      f_out.write(l + ";\n")
    end
  end
end
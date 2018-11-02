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
  file_list: [],
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
  do_tables: true,
  is_https: false,
  link_url: ""
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
  --link [url]              Downloads json from an https connection instead of using local file

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
    $config_vars[:schema] = "#{ARGV[i+1]}."
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
  when '--link'
    $config_vars[:is_https] = true
    $config_vars[:link_url] = ARGV[i+1]
  else
    if ARGV[i-1].slice(0,2) != "--" && ARGV[i].slice(0,2) != "--"
      $config_vars[:file_list] << arg
    end
  end
end

if $config_vars[:db_out] == true
  hit_issue = false
  if $config_vars[:db_host] == ""
    hit_issue = true
    puts "Must provide a host when using db connection"
  end
  if $config_vars[:db_name] == ""
    hit_issue = true
    puts "Must provide a database name"
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

file_list = []
if !$config_vars[:is_https]
  if  $config_vars[:file_list].length == 0
    puts "Please enter a valid file name"
    puts "See json2sql --help"
    exit
  end

  $config_vars[:file_list].each do |fname|
    file_list << wildcard_fopen(fname)
  end
  file_list = file_list.flatten

  if file_list.length < $config_vars[:file_list].length
    puts "Invalid file name as argument"
    puts "See json2sql --help"
    exit
  end
else
  file_list << https_open_link($config_vars[:link_url])
end

saved_state = load_schema_data($config_vars[:schema])
original_state = load_schema_data($config_vars[:schema])

system('cls') || system('clear')

puts <<~HEREDOC

    json2sql Copyright (C) 2018 Gary Boyd
    This program comes with ABSOLUTELY NO WARRANTY.
    This is free software, and you are welcome to redistribute it
    under certain conditions.

HEREDOC

begin
  Dir.mkdir("#{$config_vars[:schema].chomp(".")}-inserts") #Try and make directory
rescue => exception
  #Continue if it already exists
end

file_tables = saved_state[:table_data]
file_columns = saved_state[:column_data]
file_entries = []
item_counter = saved_state[:rel_id]
file_list.each do |file|
  cur_file = file[:file_name] + "." + file[:file_extension]
  if $config_vars[:is_https]
    file_lines = file[:file].length
  else
    file_lines = file[:file].count
    file[:file].rewind
  end


  line_counter = 1
  file[:file].each do |json_data|
    print "Parsing entry: #{line_counter}/#{file_lines} [#{cur_file}]\r"
 
    t_tables, t_columns, t_entries = create_entries_from_json(JSON.parse(json_data), item_counter)

    file_tables << t_tables
    file_columns << t_columns

    file_tables = file_tables.flatten.uniq
    file_columns = file_columns.flatten

    file_columns.each_index do |i|
      big = file_columns.select { |c| c[:table] == file_columns[i][:table] && c[:name] == file_columns[i][:name]}.max_by { |b| b[:size] }
      file_columns[i][:size] = big[:size]
    end
    file_columns = file_columns.uniq { |c| c.values_at(:name, :table)}

    if $config_vars[:do_insert] == true
      if $config_vars[:db_out]
        puts "Sending inserts via sqlcmd."
        create_insert_queries(t_entries, t_tables) do |l|
          cmd = create_sqlcmd(l, $config_vars)
          if !system(cmd)
            puts "Command error: #{cmd}"
          end
        end
      else
        out_name = "./#{$config_vars[:schema].chomp(".")}-inserts/i-#{item_counter}-#{$config_vars[:schema]}sql"
        f_out = File.new(out_name, 'ab')
        create_insert_queries(t_entries, t_tables) do |l|
          f_out.write(l + ";\n")
        end
        f_out.write("INSERT INTO #{$config_vars[:schema]}raw_data (product_id,data) VALUES(#{item_counter},'#{escape_str(json_data)}');\n")
        f_out.close
      end
    end

    line_counter += 1
    item_counter += 1
    $stdout.flush

    t_tables, t_columns, t_entries = nil
  end
  if !$config_vars[:is_https]
    file[:file].close
  end

  puts "File complete: #{cur_file}                         "
  saved_state[:rel_id] = item_counter
  saved_state[:table_data] = file_tables
  saved_state[:column_data] = file_columns
  binding.pry
  save_schema_data($config_vars[:schema], saved_state)
end

if $config_vars[:do_tables] == true
  if $config_vars[:db_out]
    puts "Sending data via sqlcmd."
    create_table_queries(file_tables, file_columns) do |l|
      cmd = create_sqlcmd(l, $config_vars)
      if !system(cmd)
        puts "Command error: #{cmd}"
      end
    end
  else
    out_name = "c-#{$config_vars[:schema]}sql"
    if $config_vars[:do_tables]
      puts "Writing headers to [#{out_name}]\r"
      f_out = File.new(out_name, 'ab')
      f_out.rewind
      create_table_queries(file_tables, file_columns) do |l|
        f_out.write(l + ";\n")
      end
      f_out.write("\n")
    end
  end
end
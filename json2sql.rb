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

puts <<~HEREDOC

    json2sql Copyright (C) 2018 Gary Boyd
    This program comes with ABSOLUTELY NO WARRANTY.
    This is free software, and you are welcome to redistribute it
    under certain conditions.

HEREDOC

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
    t_tables, t_columns, t_entries = create_entries_from_json(JSON.parse(json_data))

    file_tables << t_tables
    file_columns << t_columns
    file_entries << t_entries

    line_counter += 1
    $stdout.flush
  end
  file[:file].close

  puts "File complete: #{cur_file}"

  if line_counter > 1
    f_out = File.new(file[:file_name] + ".txt", 'w')
    create_table_queries(file_tables, file_columns).each do |l|
      f_out.write(l + ";\n")
    end
    f_out.write("\n")
    create_insert_queries(file_entries, file_tables, 1).each do |l|
      f_out.write(l + ";\n")
    end
  end
end
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

require 'open-uri'
require 'json'
require 'pry'

def sort_column_list(column_list)
  list = column_list.map(&:clone).flatten

  list.each_index do |i|
    big = list.select { |c| c[:table] == list[i][:table] && c[:name] == list[i][:name]}.max_by { |b| b[:size] }
    list[i][:size] = big[:size]
  end

  list.uniq { |c| c.values_at(:name, :table)}
end

def load_schema_data(schema_name)
  f = File.open("session-#{schema_name}json", "a+")
  if f.count > 0
    f.rewind
    data = ""
    f.each_line do |line|
      data += line
    end

    f.close
    return JSON.parse(data, symbolize_names: true)
  end
  f.close
  return {rel_id: 1, multi_session: 0, table_data: [], column_data: []}
end

def save_schema_data(schema_name, data)
  f = File.open("session-#{schema_name}json", "w")
  f.write(data.to_json)
  f.close
end

def wildcard_fopen(file_string)
  return_files = []
  Dir.glob(file_string).each do |fname|
    f = fname.split(".")
    fmain = f[0] || ""
    fext = f[1] || ""
    return_files << {file_name: fmain, file_extension:fext, file: File.open(fname, "r+")}
  end

  return_files
end

def https_open_link(url)
  file_name = url.split("/").last
  first = file_name.split(".").first
  ext = file_name.split(".").last
  data = open(url).read.split("\n")
  return {file_name: first, file_extension: ext, file: data}
end

def create_sqlcmd(query, vars)
  command = "sqlcmd -S#{vars[:db_host]} -d#{vars[:db_name]} -U'#{vars[:db_user]}' -P'#{vars[:db_pass]}' -x -I -Q \"#{query}\" > /dev/null 2>&1"
  command
end
## Workhorse method. Accept a hash as val and create all of our information
## Recursively generates:
##    [Array of Strings]  created_tables - List of all tables
##    [Array of Hashes]   created_columns - Holds column information. Including parent table, column name, and maximum size
##    [Array of Hashes]   values - Entries that need to be inserted. Including parent table, parent column, data for entry, and relational id
def create_entries_from_json(val, rel_id=1, current_table="", current_column="", nest_id=0, created_tables=[], created_columns=[], values=[])
  if val.class == Array
      val.each do |v|
        created_tables, created_columns, values = create_entries_from_json(v, rel_id, current_table, current_column, nest_id + 1, created_tables, created_columns, values)
      end
  elsif val.class == Hash
      val.each_pair do |k, v|
        if k.class != String 
          k = k.to_s
        end
        if k == ""
          k = "cvalue"
        end
        k = "c#{escape_str(k)}".gsub("'", "").gsub("\\", "")

        if !created_tables.include?(k) && nest_id == 0
            created_tables << k
            current_table = k
        else
          if created_columns.select { |c| c[:name] == k && c[:table] == current_table}.length == 0
            created_columns << {table: current_table, name: k, size: 0}
          end
          current_column = k
        end

        created_tables, created_columns, values = create_entries_from_json(v, rel_id, current_table, current_column, nest_id + 1, created_tables, created_columns, values)
      end
  else
      if current_column == ""
          current_column = "cvalue"
      end
      # p created_columns
      # p current_column
      if created_columns.select { |c| c[:name] == current_column && c[:table] == current_table}.length == 0
        created_columns << {table: current_table, name: current_column, size: 0}
      end
      
      if val.class != String
        val = val.to_s
      end
      val = escape_str(val)
      values << {table_name: current_table.to_s, column_name: current_column, value: val, column_size: val.length, relation_id: rel_id}
      # created_columns.select { |c| c[:name] == current_column && c[:table] == current_table.to_s}.each_index do |i|
      #   created_columns
      # end
      created_columns.each_index do |index|
        if created_columns[index][:name] == current_column && created_columns[index][:table] == current_table.to_s
          if created_columns[index][:size] < (val.length * 1.2).floor
            created_columns[index][:size] = (val.length * 1.2).floor
          end
        end
      end
  end

  created_tables = created_tables.flatten.uniq
  return created_tables, created_columns, values
end

## Altar and create new tables if needed on multi-session job
def alter_table_queries(old_state, new_state)
  old_tables = old_state[:table_data].clone.flatten
  new_tables = new_state[:table_data].clone.flatten
  tables_to_create = (old_tables + new_tables).flatten.uniq

  old_columns = old_state[:column_data].clone.flatten
  new_columns = new_state[:column_data].clone.flatten

  merged_columns = sort_column_list(old_columns + new_columns)
  changed_columns = merged_columns - old_columns

  changed_columns.each do |col|
    
  end
  binding.pry
end

## Create table SQL queries and yield them out to a block
def create_table_queries(table_list, column_list)
  create_statements = []
  column_list = column_list.flatten
  table_list = table_list.flatten.uniq
  table_list.each do |table|
    col_count = 0
    table_cols = []

    create_query = "CREATE TABLE #{$config_vars[:schema]}#{table} (id bigint IDENTITY(1,1) PRIMARY KEY,product_id bigint,"

    cols = column_list.select { |c| c[:table] == table}.uniq { |u| u[:name] }
    cols.each_index do |i|
      big = column_list.select { |c| c[:table] == table && c[:name] == cols[i][:name]}.max_by { |b| b[:size] }
      cols[i][:size] = big[:size]
    end

    cols.each do |c|
      if c[:size] > 7500
        create_query += "#{c[:name]} text,"
      else
        sz = c[:size]
        if sz < 10
          sz = 10
        end
        create_query += "#{c[:name]} varchar(#{sz}),"
      end
    end
    create_query = create_query.chomp(",") + ")"

    # Send completed query to block to be handled
    yield create_query
  end

  # Create extra table for raw data, original json line
  yield "CREATE TABLE #{$config_vars[:schema]}raw_data (id bigint IDENTITY(1,1) PRIMARY KEY, product_id bigint, data text)"
end

## Create insert queries and yield them to block
def create_insert_queries(entries, tables)
  table_cols = []
  table_vals = []
  product_id = 1
  last_table = tables[0]
  tables = tables.flatten.uniq
  entries = entries.flatten


  insert_queries = []
  tables.each do |table|
    cur_entries = entries.select { |k| k[:table_name] == table}
    cur_row = []
    cur_entries.each_with_index do |e,ind|
      if cur_row.select { |k| k[:column_name] == e[:column_name]}.length == 0
        cur_row << e
      end

      if ind == cur_entries.length-1 || cur_row.select { |k| k[:column_name] == cur_entries[ind+1][:column_name]}.length > 0
        statement = "INSERT INTO #{$config_vars[:schema]}#{table} (product_id,"
        cur_row.each do |r|
          statement += "#{r[:column_name]},"
        end
        statement = statement.chomp(",") + ") VALUES(#{e[:relation_id]},"
        cur_row.each do |r|
          statement += "'#{r[:value]}',"
        end
        statement = statement.chomp(",") + ")"

        cur_row = []

        # Send completed query to block to be handled
        yield statement
      end
    end
  end
end
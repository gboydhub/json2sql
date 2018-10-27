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

require 'json'

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

def create_entries_from_json(val, current_table="", current_column="", nest_id=0, created_tables=[], created_columns=[], values=[])
  if val.class == Array
      val.each do |v|
        created_tables, created_columns, values = create_entries_from_json(v, current_table, current_column, nest_id, created_tables, created_columns, values)
      end
  elsif val.class == Hash
      val.each_pair do |k, v|
        if k.class != String 
          k = k.to_s
        end

        if !created_tables.include?(k) && nest_id == 0
            created_tables << k
            current_table = k
        else
          if k == ""
            k = "value"
          end
          created_columns << {table: current_table, name: k, size: 0}
          current_column = k
        end

        created_tables, created_columns, values = create_entries_from_json(v, current_table, current_column, nest_id + 1, created_tables, created_columns, values)
      end
  else
      if current_column == ""
          current_column = "value"
      end
      # p created_columns
      # p current_column
      if created_columns.select { |c| c[:name] == current_column && c[:table] == current_table}.length == 0
        created_columns << {table: current_table.to_s, name: current_column, size: 0}
      end
      
      # p "vtable #{current_table}"
      # p "vcol #{current_column}"
      if val.class != String
        val = val.to_s
      end
      val = val.gsub("'", "\\\\'")
      values << {table_name: current_table.to_s, column_name: current_column, value: val, column_size: val.length}
      created_columns.each_index do |index|
        if created_columns[index].keys.first.to_s == current_column
          if created_columns[index][:size] < (val.length * 1.2).floor
            created_columns[index][:size] = (val.length * 1.2).floor
          end
        end
      end
      #p values.last
  end

  created_columns.each_index do |i|
    biggest = values.select { |k| k[:column_name] == created_columns[i][:name] }.max_by { |e| e[:column_size]}
    created_columns[i][:size] = (biggest[:column_size] * 2).floor
  end
  return created_tables, created_columns, values
end

def create_table_queries(table_list, column_list)
  create_statements = []
  table_list.each do |table|
    col_count = 0
    table_cols = []

    create_query = "CREATE TABLE #{table} (id bigint,product_id bigint,"

    cols = column_list.select { |k| k[:table] == table}.uniq { |h| h[:name] }
    cols.each do |c|
      create_query += "#{c[:name]} varchar(#{c[:size]}),"
    end

    create_query = create_query.chomp(",") + ")"
    create_statements << create_query
  end

  create_statements
end

def create_insert_queries(entries, tables, product_id)
  table_cols = []
  table_vals = []
  current_product_id = 1
  last_table = tables[0]

  insert_queries = []
  tables.each do |table|
    cur_entries = entries.select { |k| k[:table_name] == table}
    cur_row = []
    cur_entries.each_with_index do |e,ind|
      if cur_row.select { |k| k[:column_name] == e[:column_name]}.length == 0
        cur_row << e
      end

      if ind == cur_entries.length-1 || cur_row.select { |k| k[:column_name] == cur_entries[ind+1][:column_name]}.length > 0
        statement = "INSERT INTO #{table} (product_id,"
        cur_row.each do |r|
          statement += "#{r[:column_name]},"
        end
        statement = statement.chomp(",") + ") VALUES(#{product_id},"
        cur_row.each do |r|
          statement += "'#{r[:value]}',"
        end
        statement = statement.chomp(",") + ")"

        cur_row = []
        insert_queries << statement
      end
    end
  end

  insert_queries
end
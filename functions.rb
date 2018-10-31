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

def create_entries_from_json(val, rel_id=1, current_table="", current_column="", nest_id=0, created_tables=[], created_columns=[], values=[])
  if val.class == Array
      val.each do |v|
        created_tables, created_columns, values = create_entries_from_json(v, rel_id, current_table, current_column, nest_id, created_tables, created_columns, values)
      end
  elsif val.class == Hash
      val.each_pair do |k, v|
        if k.class != String 
          k = k.to_s
        end
        if k == ""
          k = "cvalue"
        end
        k = "c#{escape_str(k)}"

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
      
      # p "vtable #{current_table}"
      # p "vcol #{current_column}"
      if val.class != String
        val = val.to_s
      end
      val = escape_str(val)#escape_str(val)
      values << {table_name: current_table.to_s, column_name: current_column, value: val, column_size: val.length, relation_id: rel_id}
      created_columns.select { |c| c[:name] == current_column && c[:table] == current_table.to_s}.each_index do |i|
        created_columns
      end
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

def create_table_queries(table_list, column_list)
  create_statements = []
  # p table_list
  # p column_list
  column_list = column_list.flatten
  table_list = table_list.flatten.uniq
  table_list.each do |table|
    col_count = 0
    table_cols = []

    #puts "-----#{table}-----"
    create_query = "CREATE TABLE #{$config_vars[:schema]}#{table} (id bigint IDENTITY(1,1) PRIMARY KEY,product_id bigint,"

    #p column_list.flatten.select { |k| k[:table] == table}
    #p column_list.select { |k| k[:table] == table}
    # c_list = column_list.select { |k| k[:table] == table}.uniq { |h| h[:name] }
    # p c_list
    # c_list.each_index do |i|
    #   p values.select { |h| h[:column_name] == c_list[i][:name] && h[:table_name] == c_list[i][:table]}.max_by { |m| m[:column_size]}
    # end
    cols = column_list.select { |c| c[:table] == table}.uniq { |u| u[:name] }
    cols.each_index do |i|
      big = column_list.select { |c| c[:table] == table && c[:name] == cols[i][:name]}.max_by { |b| b[:size] }
      cols[i][:size] = big[:size]
    end
    #cols = cols.uniq { |h| h[:name] }#.max_by { |c| c[:size] }#.first#uniq { |h| h[:name] }
    cols.each do |c|
      if c[:size] > 7500
        create_query += "#{c[:name]} text,"
      else
        create_query += "#{c[:name]} varchar(#{c[:size]}),"
      end
    end
    #p cols
    #create_query += "#{cols[:name]} varchar(#{cols[:size]}),"

    create_query = create_query.chomp(",") + ")"
    #create_statements << create_query
    yield create_query
  end

  yield "CREATE TABLE #{$config_vars[:schema]}raw_data (id bigint IDENTITY(1,1) PRIMARY KEY, product_id bigint, data text)"

  #create_statements
end

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
        yield statement
      end
    end
  end

  #insert_queries
end
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

require 'test/unit'

require_relative '../functions.rb'

class TestSimpleNumber < Test::Unit::TestCase

  def test_wildcard_file_open
    assert_equal(Array, wildcard_fopen("test*.txt").class)
    assert_equal([], wildcard_fopen("nofile*.txt"))

    assert_equal(2, wildcard_fopen("test*.txt").length)

    assert_equal(File, wildcard_fopen("test*.txt").first[:file].class)
    assert_equal(File, wildcard_fopen("test*.txt").last[:file].class)
  end

  def test_create_entries_type
    test_json = {a: "22"}
    assert_equal(Array, create_entries_from_json(test_json).class)

    t_tables, t_columns, t_entries = create_entries_from_json(test_json)
    assert_equal(2, t_entries.first[:column_size])
    assert_equal("22", t_entries.first[:value])
    assert_equal("value", t_entries.first[:column_name])
    assert_equal("a", t_entries.first[:table_name])
  end

  def test_create_entries_hash
    test_json = {reviews: {user: "tester", review: "Great test's"}}
    t_tables, t_columns, t_entries = create_entries_from_json(test_json)

    assert_equal(6, t_entries.first[:column_size])
    assert_equal("tester", t_entries.first[:value])
    assert_equal("user", t_entries.first[:column_name])
    assert_equal("reviews", t_entries.first[:table_name])
    
    assert_equal("Great test\\'s", t_entries.last[:value])
    assert_equal("review", t_entries.last[:column_name])
    assert_equal("reviews", t_entries.last[:table_name])
  end

  def create_entries_array
    test_json = {upcs: ["1234", "1337"]}
    t_tables, t_columns, t_entries = create_entries_from_json(test_json)
    assert_equal(4, t_entries.first[:column_size])
    assert_equal("1234", t_entries.first[:value])
    assert_equal("value", t_entries.first[:column_name])
    assert_equal("upcs", t_entries.first[:table_name])

    assert_equal(4, t_entries.last[:column_size])
    assert_equal("1337", t_entries.last[:value])
    assert_equal("value", t_entries.last[:column_name])
    assert_equal("upcs", t_entries.last[:table_name])
  end

  def test_create_from_files
    file_list = wildcard_fopen("test*.txt")

    assert_equal("test1", file_list.first[:file_name])

    t_tables, t_columns, t_entries = create_entries_from_json(JSON.parse(file_list.first[:file].readlines.first))
    assert_equal(4, t_entries.first[:column_size])
    assert_equal("1234", t_entries.first[:value])
    assert_equal("value", t_entries.first[:column_name])
    assert_equal("upcs", t_entries.first[:table_name])

    assert_equal(4, t_entries.last[:column_size])
    assert_equal("1337", t_entries.last[:value])
    assert_equal("value", t_entries.last[:column_name])
    assert_equal("upcs", t_entries.last[:table_name])
  end

  def test_create_table_queries
    test_json = {reviews: [{user: "tester", review: "Great test"},{user: "test", review: "Great"}]}
    t_tables, t_columns, t_entries = create_entries_from_json(test_json)
    table_statements = create_table_queries(t_tables, t_columns)

    assert_equal(1, table_statements.length)
    assert_equal("CREATE TABLE reviews (id bigint,product_id bigint,user varchar(12),review varchar(20))", table_statements.first)

    test_json = {name: "Cool app", reviews: [{user: "tester", review: "Great test's"},{user: "test", review: "Great"}]}
    t_tables, t_columns, t_entries = create_entries_from_json(test_json)
    table_statements = create_table_queries(t_tables, t_columns)
    
    assert_equal(2, table_statements.length)
    assert_equal("CREATE TABLE name (id bigint,product_id bigint,value varchar(16))", table_statements[0])
    assert_equal("CREATE TABLE reviews (id bigint,product_id bigint,user varchar(12),review varchar(26))", table_statements[1])
  end

  def test_create_insert_queries
    test_json = {reviews: [{user: "tester", review: "Great test's"},{user: "test", review: "Great"}]}
    t_tables, t_columns, t_entries = create_entries_from_json(test_json)
    insert_statements = create_insert_queries(t_entries, t_tables, 1)

    assert_equal("INSERT INTO reviews (product_id,user,review) VALUES(1,'tester','Great test\\'s')", insert_statements.first)
    assert_equal("INSERT INTO reviews (product_id,user,review) VALUES(1,'test','Great')", insert_statements.last)

    test_json = {name: "Cool app", reviews: [{user: "tester", review: "Great test's"},{user: "test", review: "Great"}]}
    t_tables, t_columns, t_entries = create_entries_from_json(test_json)
    insert_statements = create_insert_queries(t_entries, t_tables, 1)

    assert_equal("INSERT INTO name (product_id,value) VALUES(1,'Cool app')", insert_statements[0])
    assert_equal("INSERT INTO reviews (product_id,user,review) VALUES(1,'tester','Great test\\'s')", insert_statements[1])
    assert_equal("INSERT INTO reviews (product_id,user,review) VALUES(1,'test','Great')", insert_statements[2])
  end
 
end
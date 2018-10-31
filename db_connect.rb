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

require 'tiny_tds'
require 'mysql2'

def create_client_from_config()
    unless $config_vars[:db_out]
        return nil
    end

    case $config_vars[:db_type].downcase
    when 'azure'
        client = TinyTds::Client.new username: $config_vars[:db_user], password: $config_vars[:db_pass],
                host: $config_vars[:db_host], port: $config_vars[:db_port].to_i, database: $config_vars[:db_name], azure: true
        unless client.active?
            return nil
        end

        results = client.execute("SET ANSI_NULLS ON")  
        results = client.execute("SET CURSOR_CLOSE_ON_COMMIT OFF")  
        results = client.execute("SET ANSI_NULL_DFLT_ON ON")  
        results = client.execute("SET IMPLICIT_TRANSACTIONS OFF")  
        results = client.execute("SET ANSI_PADDING ON")  
        results = client.execute("SET QUOTED_IDENTIFIER ON")  
        results = client.execute("SET ANSI_WARNINGS ON")  
        results = client.execute("SET CONCAT_NULL_YIELDS_NULL ON")  
        return client
    end
end

def escape_str(str)
    Mysql2::Client.escape(str).gsub("'","''")
end
for %%G in (./inserts/*.sql) do @(sqlcmd -S localhost,1433 -E -x -d master -i "./inserts/%%G") >> log.txt
# 🚀 GocqlX Migrations

`migrate` reads migrations from a flat directory containing CQL files.
There is no imposed naming schema. Migration name is file name.
The order of migrations is the lexicographical order of file names in the directory. 
You can inject execution of Go code before processing of a migration file, after processing of a migration file, or between statements in a migration file.

For details see [example](example) migration.
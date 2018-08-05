#!/bin/bash -x

echo Unmounting for now just destroys everything in the driver

sg unmount test_mount
sg unmount test_mount_mongo
sg unmount output
sg unmount output_pull

echo Mount a postgres and a mongo source.

sg mount -c originro:originpass@pgorigin:5432 -h postgres_fdw -o "{\"dbname\": \"origindb\", \"remote_schema\": \"public\"}" test_mount
sg mount -c originro:originpass@mongoorigin:27017 -h mongo_fdw -o "{\"stuff\": {\"db\": \"origindb\", \"coll\": \"stuff\", \"schema\": {\"name\": \"text\", \"duration\": \"numeric\", \"happy\": \"boolean\"}}}" test_mount_mongo

echo Pretend to commit the starting snaps to give them reproducible "hashes"

sg commit test_mount -h cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe
sg commit test_mount_mongo -h cafecafecafecafecafecafecafecafecafecafecafecafecafecafecafecafe

echo Some status commands
sg status
sg status test_mount

echo Query the db
PGPASSWORD=supersecure psql postgresql://localhost:5432/cachedb -U clientuser -c "select * from test_mount.fruits"

echo Or use the shorthand
sg sql "select * from test_mount.fruits"
sg sql "select * from test_mount_mongo.stuff"

echo Cross-db joins work
sg sql "select test_mount.fruits.fruit_id, test_mount_mongo.stuff.name, test_mount.fruits.name as spirit_fruit from test_mount.fruits join test_mount_mongo.stuff on test_mount.fruits.fruit_id = test_mount_mongo.stuff.duration"

echo Change it
sg sql "insert into test_mount.fruits values (3, 'mayonnaise')"
sg sql "select * from test_mount.fruits"

echo Commit the result with a overridden predictable hash
sg commit test_mount -h daefabbeeffacedddaefabbeeffacedddaefabbeeffacedddaefabbeeffacedd

sg diff test_mount cafebabe daefab -v

echo Checkout the old commit -- no mayonnaise!
sg checkout test_mount cafe
sg sql "select * from test_mount.fruits"

echo A rudimentary sgfile implementation
cat test/resources/sample.sgfile
sg file test/resources/sample.sgfile
sg sql "select * from output.fruits"

sg status output
sg log output

echo A tree view seems to be supported, too
sg log -t output

echo We can clone/pull as well: let\'s pull changes from ourselves
sg clone clientuser:supersecure@localhost:5432/cachedb output output_pull
sg log -t output_pull

echo Executing the sgfile checks out the final commit on output, but it\'s based on the 00000000 hash, so executing it again will reuse the cache completely.
sg file test/resources/sample.sgfile

echo Finally, now we base the file execution on the version of test_mount with mayonnaise \(daefab...\)
sg checkout test_mount daefab
sg file test/resources/sample.sgfile

sg log -t output
sg sql "select * from output.fruits"

echo Also store a full snapshot together with the diff.
echo Note schema changes aren''t supported yet.
sg sql "insert into output.fruits values (4, 'yeast')"
sg sql "delete from output.fruits where name = 'mayonnaise'"
sg commit -s -h ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff output
sg diff output ffffff -v

# output_pull hasn't changed yet, obviously
sg log -t output_pull
sg pull output_pull
sg log -t output_pull


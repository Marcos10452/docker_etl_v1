
https://stackoverflow.com/questions/35679995/how-to-use-a-postgresql-container-with-existing-data

docker-compose.yml
version: '2'

services:
  postgres9:
    image: postgres:9.4
    expose:
      - 5432
    volumes:
      - data:/var/lib/postgresql/data

volumes:
  data: {}
demo
Start Postgres database server:

$ docker-compose up
Show all tables in the database. In another terminal, talk to the container's Postgres:

$ docker exec -it $(docker-compose ps -q postgres9 ) psql -Upostgres -c '\z'
It'll show nothing, as the database is blank. Create a table:

$ docker exec -it $(docker-compose ps -q postgres9 ) psql -Upostgres -c 'create table beer()'
List the newly-created table:

$ docker exec -it $(docker-compose ps -q postgres9 ) psql -Upostgres -c '\z'

                         Access privileges
 Schema |   Name    | Type  | Access privileges | Column access privileges 
--------+-----------+-------+-------------------+--------------------------
 public | beer      | table |                   | 
Yay! We've now started a Postgres database using a shared storage volume, and stored some data in it. Next step is to check that the data actually sticks around after the server stops.

Now, kill the Postgres server container:

$ docker-compose stop
Start up the Postgres container again:

$ docker-compose up
We expect that the database server will re-use the storage, so our very important data is still there. Check:

$ docker exec -it $(docker-compose ps -q postgres9 ) psql -Upostgres -c '\z'
                         Access privileges
 Schema |   Name    | Type  | Access privileges | Column access privileges 
--------+-----------+-------+-------------------+--------------------------
public | beer      | table |                   | 
We've successfully used a new-style Docker Compose file to run a Postgres database using an external data volume, and checked that it keeps our data safe and sound.

storing data
First, make a backup, storing our data on the host:

$ docker exec -it $(docker-compose ps -q postgres9 ) pg_dump -Upostgres > backup.sql
Zap our data from the guest database:

$ docker exec -it $(docker-compose ps -q postgres9 ) psql -Upostgres -c 'drop table beer'
Restore our backup (stored on the host) into the Postgres container.

Note: use "exec -i", not "-it", otherwise you'll get a "input device is not a TTY" error.

$ docker exec -i $(docker-compose ps -q postgres9 ) psql -Upostgres < backup.sql
List the tables to verify the restore worked:

$ docker exec -it $(docker-compose ps -q postgres9 ) psql -Upostgres -c '\z'
                         Access privileges
Schema |   Name    | Type  | Access privileges | Column access privileges 
--------+-----------+-------+-------------------+--------------------------
public | beer      | table |                   | 
To sum up, we've verified that we can start a database, the data persists after a restart, and we can restore a backup into it from the host.

Thanks Tomasz!
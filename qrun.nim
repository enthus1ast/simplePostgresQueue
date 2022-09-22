import db_postgres, sequtils, os

const
  SERVER = "127.0.0.1"
  USER = "postgres"
  PW = "PASSWORD"
  DATABASE= "pipelines"

template withRow(db: DbConn, body: untyped) =
  ## locks one row, the row is injected into the body with `row`
  db.exec(sql"BEGIN;")
  var row {.inject.} = db.getRow(sql"SELECT * FROM queue_table LIMIT 1 FOR UPDATE SKIP LOCKED;")
  body
  db.exec(sql"COMMIT;")

proc ack(db: DbConn, id: string) =
  # call this if the work was sucessfully
  db.exec(sql"delete from queue_table where id = ?", id)

when isMainModule:
  import random
  var db = open(SERVER, USER, PW, DATABASE)
  while true:
    db.withRow:
      echo row
      sleep(10000)
      if rand(1) == 1:
        db.ack(row[0])
      else:
        echo "Simulate failure", row[0]


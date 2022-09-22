# TODO
# - Add option to change the table_name
# - Add option to change the payload datatype (jsonb?)
# - Block checkout somehow so that no polling is needed.

import psycopg
import time

class QRun:
  def __init__(self, server, user, password, database, table = "queue_table"):
    self.server = server
    self.user = user
    self.password = password
    self.database = database
    self.dbconn = None
    self.cur = None
    self.checkoutRowId = None

  def _testGood(self):
    if self.dbconn == None or self.cur == None :
      raise ConnectionError("Connect to db first!")

  def connect(self):
    """Connect to the database"""
    self.dbconn = psycopg.connect(
      host = self.server,
      user = self.user,
      password = self.password,
      dbname = self.database
    )
    self.cur = self.dbconn.cursor()

  def _hasCheckout(self):
    return self.checkoutRowId != None

  def checkout(self):
    """Returns and locks a row."""
    self._testGood()
    if self._hasCheckout():
      raise RuntimeError("Already checked out a row, call `ack` or `nack` before checking out a new one")
    self.cur.execute("BEGIN;")
    self.cur.execute("SELECT * FROM queue_table LIMIT 1 FOR UPDATE SKIP LOCKED;")
    row = self.cur.fetchone()
    if row == None:
      raise ValueError("No work in queue.")
    else:
      self.checkoutRowId = row[0]
    return row

  def ack(self):
    """Acknowlege the row, remove it from the queue"""
    self._testGood()
    if not self._hasCheckout():
      raise RuntimeError("Nothing checked out!")
    self.cur.execute(
      "delete from queue_table where id = %s",
      [self.checkoutRowId]
    )
    self.cur.execute("COMMIT;")
    self.checkoutRowId = None

  def addWork(self, payload):
    """Adds work to the queue, work must be a string"""
    self._testGood()
    self.cur.execute("insert into queue_table (payload) VALUES (%s);", [payload]) # TODO why self.database does not work as a placeholder?
    self.cur.execute("commit") # TODO why self.database does not work as a placeholder?

  def nack(self):
    """DO NOT Acknowlage the row, leave it in the queue, rollback all db changes"""
    self._testGood()
    self.cur.execute("ROLLBACK;")
    self.checkoutRowId = None

  def close(self):
    self._testGood()
    self.cur.close()
    self.dbconn.close()
    self.cur = None
    self.dbconn = None

  def createTable(self):
    self._testGood()
    self.cur.execute("""
      CREATE TABLE queue_table (
          id int not null primary key generated always as identity,
          queue_time timestamptz default now(),
          payload	text
      );
    """)
    self.cur.execute("commit;")

  def _addTestData(self):
    self._testGood()
    self.cur.execute("INSERT INTO queue_table (payload) SELECT 'data-' || text(generate_series(1,1000));")
    self.cur.execute("commit;")


if __name__ == "__main__":
  SERVER = "127.0.0.1"
  USER = "postgres"
  PW = "PASSWORD"
  DATABASE= "pipelines"
  qrun = QRun(SERVER, USER, PW, DATABASE)
  qrun.connect()
  qrun.createTable()

  while True:
    try:
      print(qrun.checkout())

      ## DO YOUR WORK HERE
      time.sleep(5)
      good = True


      if good:
        qrun.ack()
      else:
        qrun.nack()
    except ValueError:
      print("No work")
      time.sleep(1)

  qrun.close()
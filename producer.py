from qrun import QRun
import time

if __name__ == "__main__":
  SERVER = "127.0.0.1"
  USER = "postgres"
  PW = "PASSWORD"
  DATABASE= "pipelines"
  qrun = QRun(SERVER, USER, PW, DATABASE)
  qrun.connect()

  while True:
    qrun.addWork("TEST")
    print("ADDED")
    time.sleep(2)

  # print(qrun.checkout())
  # qrun.ack()
  qrun.close()
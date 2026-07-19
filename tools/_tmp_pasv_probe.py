import sys, re, time
sys.path.insert(0, '/opt/airflow/dags')
import ftplib
from configs import getConfig

config = getConfig()
server = ftplib.FTP(timeout=10)
server.connect(config["FtpHost"], config["FtpPort"])
server.login(config["FtpUser"], config["FtpPassword"])
resp = server.sendcmd('PASV')
m = re.search(r'\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)', resp)
port = int(m.group(5)) * 256 + int(m.group(6))
print(f"PASV_PORT={port}", flush=True)
# leave the control connection open (don't quit) so the passive
# listener stays valid while the host is checked for a LISTEN socket
time.sleep(8)
print("done waiting", flush=True)

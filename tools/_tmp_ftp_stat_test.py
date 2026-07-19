import sys, socket, re, time
sys.path.insert(0, '/opt/airflow/dags')
import ftplib
from configs import getConfig

config = getConfig()
results = []
for i in range(20):
    try:
        server = ftplib.FTP(timeout=10)
        server.connect(config["FtpHost"], config["FtpPort"])
        server.login(config["FtpUser"], config["FtpPassword"])
        resp = server.sendcmd('PASV')
        m = re.search(r'\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+)\)', resp)
        ip = '.'.join(m.groups()[:4])
        port = int(m.group(5)) * 256 + int(m.group(6))
        t0 = time.time()
        try:
            s = socket.create_connection((ip, port), timeout=3)
            s.close()
            results.append((port, 'OK', time.time() - t0))
        except Exception as e:
            results.append((port, f'FAIL:{type(e).__name__}', time.time() - t0))
        server.quit()
    except Exception as e:
        results.append(('N/A', f'CONTROL_FAIL:{type(e).__name__}:{e}', 0))
    time.sleep(0.5)

ok = sum(1 for r in results if r[1] == 'OK')
print(f"Success: {ok}/{len(results)}")
for port, status, elapsed in results:
    print(f"  port={port} status={status} elapsed={elapsed:.2f}s")

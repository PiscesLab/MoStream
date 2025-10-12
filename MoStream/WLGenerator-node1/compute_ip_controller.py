import subprocess

#subprocess.run(["python", "simulator.py"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
#subprocess.run(["python", "compute_ip_parsl.py"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
subprocess.run(["python", "compute_ip_parsl_multiworkers.py"], stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)

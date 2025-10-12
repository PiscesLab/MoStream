# !/usr/bin
# -*- coding:utf-8 -*-
# Copyright (c) Jun.28 2022 - Jianshu Liu <jliu96@lsu.edu>
import sys,re,time,csv,math
from subprocess import call,check_output
import numpy as np
import datetime
import time

#pwd = check_output('pwd')
compute_ip_log = "./compute_ip.log"
compute_ip_output = "./compute_ip-output.log"

#summar time zone -18000
#winter time zone -21600

def LogAnalysis():
    with open(compute_ip_log) as f, open(compute_ip_output, 'w') as w:
        fieldnames = ['timestamp', 'IP_simulate']
        writer = csv.DictWriter(w, fieldnames=fieldnames)
        for line in f:
            timestamp = float(line.split(" ")[1])/1000
            IP_sim = float(line.split(" ")[5])
            writer.writerow({'timestamp':timestamp, 'IP_simulate':IP_sim})
        

if __name__ == "__main__":
    LogAnalysis()

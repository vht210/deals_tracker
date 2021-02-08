import os
import subprocess
import sys, getopt
import traceback
import logging
import shlex
import csv
import time
from datetime import datetime
import pandas
import requests
import json
import pandas as pd
from collections import OrderedDict
#https://slingshot.filecoin.io/api/get-previous-phase-data?phase=2.1&file=deals_list_5fc274a7bb7b32dc2a0841d2.json
#https://slingshot.filecoin.io/api/get-previous-phase-data?phase=2.2&file=deals_list_5fc274a7bb7b32dc2a0841d2.json
#https://space-race-slingshot-phase2.s3-us-west-2.amazonaws.com/prod/deals_list_5fc274a7bb7b32dc2a0841d2.json


START_EPOCH = 1598306400  # Timestamp mainnet launch                                                    )

PREFIX_URL_PHASE21="https://slingshot.filecoin.io/api/get-previous-phase-data?phase=2.1&file=deals_list_"
PREFIX_URL_PHASE22="https://slingshot.filecoin.io/api/get-previous-phase-data?phase=2.2&file=deals_list_"
PREFIX_URL_PHASE23="https://space-race-slingshot-phase2.s3-us-west-2.amazonaws.com/prod/deals_list_"

LEADER_BOARD="https://slingshot.filecoin.io/api/get-previous-phase-data?phase=2.2&file=leaderboard.json"

DISPLAY_DEALS_ID=True
DEBUG = False
DEAL_SIZE=0.03125 # 32GiB=0,00390625

class DealsDup(object):
    def __init__(self,project_id):
        self.project_id = project_id
        self.url_phase21=PREFIX_URL_PHASE21+str(self.project_id) + ".json"
        self.url_phase22=PREFIX_URL_PHASE22+str(self.project_id) + ".json"
        self.url_phase23=PREFIX_URL_PHASE23+str(self.project_id) + ".json"

    def get_deal_id(self,url):
        if DEBUG:
            print("Query url => " + url)
        deals_id=[]
        try:
            rsp = requests.get(url)
            if rsp.status_code == 200:
                data = rsp.json()
                if "payload" in data:
                    payload = data["payload"]
                    if "payload" in payload: #SS2.1
                        payloadx= payload["payload"]
                        deals_id = [x["deal_id"] for x in payloadx]
                    else:
                        deals_id = [x["deal_id"] for x in payload]

        except:
            #print("Unble to query " + url)
            pass
        return deals_id

    def get_nbr_common(self,url1,url2):
        data1=self.get_deal_id(url1)
        data2=self.get_deal_id(url2)
        return list(set(data1).intersection(data2))

    def get_dup_21_22(self):
        return self.get_nbr_common(self.url_phase21, self.url_phase22)

    def get_dup_22_23(self):
        return  self.get_nbr_common(self.url_phase22,self.url_phase23)


    @staticmethod
    def epoch_to_utc_time(epoch_number):
        if epoch_number > 0:
            unix_timestamp = START_EPOCH + epoch_number * 30
            return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M')
        else:
            return -1


def get_top(top=20):
    project_name_id=OrderedDict()
    try:
        rsp = requests.get(LEADER_BOARD)
        if rsp.status_code == 200:
            data = rsp.json()
            count=0
            if "payload" in data:
                for item in data["payload"]:
                    if count > top:
                        break
                    if "name" in item and "project_id" in item:
                        project_name_id[item["name"]] = item["project_id"]
                    count=count+1
    except:
        print("Unable to parse  " + LEADER_BOARD)
        print(traceback.print_exc())
    return project_name_id
def main():
    top = get_top(top=20)
    for project,project_id in top.items():
        dd = DealsDup(project_id)
        dup_nbr = dd.get_dup_21_22()
        print("Duplicate deals Slingshot 2.1 and 2.2 for  " + project + ": " + str(len(dup_nbr)) + " (~" + str(DEAL_SIZE*len(dup_nbr)) + " TiB)" )
        if DISPLAY_DEALS_ID:
            print(dup_nbr)

    print("\n\n")
    for project, project_id in top.items():
        dd = DealsDup(project_id)
        dup_nbr = dd.get_dup_22_23()
        print("Duplicate deals Slingshot 2.2 and 2.3 for  " + project + ": " + str(len(dup_nbr)) + " (~" + str(DEAL_SIZE*len(dup_nbr)) + " TiB)" )
        if DISPLAY_DEALS_ID:
            print(dup_nbr)

if __name__ == '__main__':
    main()



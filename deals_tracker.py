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

# https://filecoin.tools/api/deals/f3s5qozdyocudxtt2ekvcbu34dj2ubt3b4fqdzjqla7gj7442xckmlntcsmzibru6zkpiqlwflgn554cdhw6va?page=1&per_page=20000

DEFAULT_INTERVAL = 24
PAGE = 1
PER_PAGE = 200000
PRE_URL = "https://filecoin.tools/api/deals/"
SUF_URL = "?page=" + str(PAGE) + "&per_page=" + str(PER_PAGE)
START_EPOCH = 1598306400  # Timestamp mainnet launch                                                    )


class DealsTracker(object):
    def __init__(self, addrs, is_on_chain, interval):
        self.addrs = addrs
        self.is_on_chain = is_on_chain
        self.interval = interval

    def get_deals_df(self):
        for addr in self.addrs:
            url = PRE_URL + addr + SUF_URL
            print("Query url => " + url)
            try:
                rsp = requests.get(url)
                if rsp.status_code == 200:
                    data = rsp.json()
                    if "Deals" in data:
                        deals = data["Deals"]
                        deal_lst = [x["DealInfo"]["Proposal"] for x in deals]
                        deal_state=[x["DealInfo"]["State"] for x in deals]
                        #print(deal_state)
                        deals_df = pd.DataFrame(deal_lst)
                        deals_df=deals_df.append(pd.DataFrame(deal_state))
                        deals_df["start_utc"] = deals_df.apply(lambda row: DealsTracker.epoch_to_utc_time(row['SectorStartEpoch']),axis=1)
                        df_provider = deals_df.groupby(["Provider"])[["VerifiedDeal"]].count()
                        df_provider["percentage"] =df_provider.apply(lambda x: x*100/x.sum())
                        print(df_provider)
                        df_label = deals_df.groupby(["Label"])[["VerifiedDeal"]].count()
                        df_label["percentage"] = df_label.apply(lambda x: x * 100 / x.sum())
                        print(df_label)

            except:
                print("Error when query " + url)
                print(str(traceback.print_exc()))

    @staticmethod
    def epoch_to_utc_time(epoch_number):
        if epoch_number > 0:
            unix_timestamp = START_EPOCH + epoch_number * 30
            return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M')
        else:
            return -1


def main(argv):
    addr_lst = list()
    on_chain = True
    interval = DEFAULT_INTERVAL
    try:
        opts, args = getopt.getopt(argv, "h:a:c:i:", ["help=", "addr=", "on_chain=", "interval="])
    except getopt.GetoptError:
        print_help()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ["-h", "--help"]:
            print_help()
            sys.exit()
        elif opt in ["-a", "--addr"]:
            addr_lst = list(str(arg).split(","))
        elif opt in ["-c", "--on_chain"]:
            on_chain = str(arg).lower() in ["yes", "true", "1"]
        elif opt in ["-i", "--interval"]:
            try:
                interval = int(arg)
            except:
                interval = DEFAULT_INTERVAL

    if len(addr_lst) > 0:
        dt = DealsTracker(addr_lst, on_chain, interval)
        dt.get_deals_df()
    else:
        print("Address is empty!")


def print_help():
    print("python3 deals_tracker --addr=addr1,add2 --on_chain=True --interval=24")
    print("option --on_chain and --interval not yet in use")

if __name__ == '__main__':
    if (len(sys.argv)) == 1:
        print_help()
        sys.exit(1)
    main(sys.argv[1:])



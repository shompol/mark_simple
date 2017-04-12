#!/usr/bin/env python
"""
inst path:

r'\\FILESERV2K\g$\Z-drive\PY\lib'
"""

import os
os.environ.pop('TZ', None)  # call this first before some library somewhere used datetime with bad windoz timezone
from random import randint
import re
import argparse
import sys
#sys.path.append(r'\\FILESERV2K\g$\Z-drive\lib')
import aig
import sayr
from dbcon import Dbcon
import oql
from oql import FROM, Tbl, JOIN, JEQ
from goodtimes import GoodTimes


class Mark:
    DATA_SOURCE_TABLE = 'str: must define in subclass'
    DATA_SOURCE_COL = 'str: must define in subclass'
    
    def __init__(self):
        srv = "EMAILDBSERV2K"
        db = "EMAIL"
        coldef = oql.ColDef('custno', 'char', 16, null=False)
        self.temp_table = oql.Tbl("mark_{:015}".format(randint(0, 1E15)), db='tempdb')
        self.dc = Dbcon(srv, db)
        self.dc(oql.CreateTable(self.temp_table, [coldef]))


    def __call__(self, inout_file):
        with GoodTimes("BCP") as gt:
            self.dc.bcp_insert(self.temp_table, inout_file)
            gt.stamp("INSERT DONE")

            fr = FROM(Tbl(self.temp_table, 't'), JOIN(Tbl(self.DATA_SOURCE_TABLE, 'h', db='email'), on=JEQ('t', 'h', 'custno'), prefix='LEFT OUTER'))
            #TODO fi select in line with new definition.
            #WAS def bcp_select(self, tbl, cond, cols, order_by=None, group_by=None, top=None, nolock=False, outfile=None):
            #self.dc.bcp_select(fr, None, ['t.custno'] + self.DATA_SOURCE_COLS, outfile=inout_file)
            self.dc.bcp_select(fr, inout_file, cols=['t.custno'] + self.DATA_SOURCE_COLS)
            gt.stamp("SELECT DONE")


    def __del__(self):
        try:
            self.dc.drop_if_exists(self.temp_table)
        except:
            pass

        
class Hygiene(Mark):
    DATA_SOURCE_TABLE = 'hygiene_imp'
    DATA_SOURCE_COLS = ['email.dbo.hygiene_code(status_code, netcode)',
                        'email.dbo.hygiene_code2(email.dbo.hygiene_code(status_code, netcode))', 'upper(country_cd)']


class FreshAddress(Mark):
    DATA_SOURCE_TABLE = 'fresh_address'
    DATA_SOURCE_COLS = ['status_code']


def main():
    #try:
    parser = argparse.ArgumentParser()
    parser.add_argument("field", choices=['h', 'hyg', 'hygiene', 'advhyg', 'fa', 'fresh_address'],
                        type=str.lower,
                        help="Column to add and populate")
    parser.add_argument("dbf_file", help="DBF file with a column 'custno' to add field to")
    args = parser.parse_args()

    marktype = dict(
        h             = Hygiene,
        hyg           = Hygiene,
        hygiene       = Hygiene,
        advhyg        = Hygiene,
        fa            = FreshAddress,
        fresh_address = FreshAddress)[args.field]

    marktype()(args.dbf_file)

    #except Exception as e:

    #    msg = str(e)
    #    msg = re.sub(aig.pwd, '', msg)
    #    sayr.say("\nTHERE WAS AN ERROR PROCESSING YOUR REQUEST:", msg)
    #    input("Press Enter to exit...")
    #    sayr.print_exc()


if __name__ == '__main__':
    main()

#!/usr/bin/env python
import dbf
import os
import dbcon
from sayr import say
from random import randint

dbffile = 'sample.DBF'


def create_sample():
    try:
        os.remove(dbffile)
    except FileNotFoundError:
        pass

    cnos = ['0000000000000001',
            '0000000000000002',
            '0000000000000003',
            '17b439d9c8bd7b96',
            '71d499911aa1579b',
            '3215132501a579ff',
            '820f685ceca87cf9',
            'b32bb660fa8af27f',
            '9ab4642db027cb75',
            '6f1f5b2214a4f553',
            '54eccfee907af70f',
            'af038b0dc968ccef',
            '0000018e15f00700',
            '00000236bf5112f7',
            '0000067395bdb075']


    table = dbf.Table(dbffile , "custno c(16)")
    with table:
        for c in cnos:
            print(c)    
            table.append((c,))




def create_big_sample():
    
    try:
        os.remove(dbffile)
    except FileNotFoundError:
        pass
    
    table = dbf.Table(dbffile , "custno c(16)")

    dc = dbcon.Dbcon('emaildbserv2k', 'email')
    
    with table:
        rows = 0
        for c in dc.select('hygiene_imp', None, 'custno', top=50000):
            if rows % 1000 == 0:
                say(rows, c)
            table.append((c,))
            table.append(("{:016x}".format(randint(0, 0xffffffffffffffff)),))
            rows += 2

    say("appended", rows, "rows to", dbffile)



if __name__ == '__main__':
    create_sample()

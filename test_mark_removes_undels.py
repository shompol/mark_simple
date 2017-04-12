#!python
import os
import unittest
import subprocess
from random import randint

import sys
sys.path.append('mark_removes_undels')

from oql import NE, Lit
from dbcon import Dbcon
from say import say, ssay





class TestOutputValid(unittest.TestCase): # pylint: disable=too-many-public-methods
    """
    campaign_id 795346

    email                         |  del  |   perm |   comment        |  command                                                |  output
    nerinah@gemcomsoftware.com    |  0    |   0    |   global remove  | select top 2 * from remove r where r.flags & 0x1=1      |  expect del=0 perm=0
    georger@midwest-air.com       |  0    |   1    |   global remove  | select top 2 * from remove r where r.flags & 0x1=1      |  expect del=0 perm=0
    leslie@mrgf.org               |  0    |   0    |   db 44 remove   | select top 2 * from remove where database_id=44         |  expect del=0 perm=0
    lessie.evans@uvestmail.com    |  0    |   1    |   db 44 remove   | select top 2 * from remove where database_id=44         |  expect del=0 perm=0
    removed@no.such.email.it      |  0    |   0    |                  |                                                         |  expect del=0 perm=0
    permitted@no.such.email.it    |  0    |   1    |                  |                                                         |  expect del=0 perm=1

    hkadoch@blumaroffshore.com    |  0    |   0    |   global undel   | select top 2 * from undeliverable where flags & 0x80 > 0|  expect del=0 perm=0
    abarry@iptimaging.com         |  1    |   0    |   global undel   | select top 2 * from undeliverable where flags & 0x80 > 0|  expect del=0 perm=0
    cd@reacontract.com            |  0    |   0    |   db 44 undel    | select top 7 * from undeliverable where database_id=44  |  expect del=0 perm=0
    kspurgeon@yisd.net            |  1    |   0    |   db 44 undel    | select top 7 * from undeliverable where database_id=44  |  expect del=0 perm=0
    undel@no.such.email.it        |  0    |   0    |                  |                                                         |  expect del=0 perm=0
    yesdel@no.such.email.it       |  1    |   0    |                  |                                                         |  expect del=1 perm=0
    """

    def get_cdb(self):
        self.cdb_id, name, lname = self.metacon.select_one('v_clientdb', NE('name','aig'), "id name long_name".split(), order_by='newid()', top=1)
        say("Testing on database {} {} {}".format(self.cdb_id, name, lname))


    def get_campaign_id(self):
        # select top 1 campaign_id from ecampaign, order_ where ecampaign.parent_id=order_.last_version_id and order_.database_id=593 order by newid()
        qry = '''
        SELECT top 1 campaign_id 
        FROM ecampaign, order_ 
        WHERE ecampaign.parent_id=order_.last_version_id AND order_.database_id={dbid} 
        ORDER BY NEWID()
        '''.format(dbid=self.cdb_id)

        row  = self.metacon.dbc.execute(qry).fetchone()
        if not row:
            say("No campaigns, try another DB")
            self.get_cdb()
            return self.get_campaign_id()

        self.campaign_id = row.campaign_id
        say('Using campaign_id', self.campaign_id)




    SAMPLE_SIZE = 10
    

    FAKE_EMAILS = [
        'dedmoroz@zaicy.com', 'babayaga@izbushka.com', 'kurochka.ryaba@babka.dedka.com',
        'dedmoroz@zaicy1.com', 'babayaga@izbushka1.com', 'kurochka.ryaba@babka1.dedka.com',
        'dedmoroz@zaicy2.com', 'babayaga@izbushka2.com', 'kurochka.ryaba@babka2.dedka.com',
        'dedmoroz@zaicy3.com', 'babayaga@izbushka3.com', 'kurochka.ryaba@babka3.dedka.com'
    ]



    BAD_EMAILS = ["", "       ", "  bademail   ", "     bad email      ", "   @   ", "       bademail@.      ",
                  "    @ba.demail     ",  " bad email@badem.ail ", " bademail@bad em.ail ", " bademail @badem.ail ", " bademail@ ba.demail "]

    @staticmethod
    def validemail(email):
        if not email:
            return False

        email = email.strip()
        if not email:
            return False

        if len(email.split()) > 1:
            return False

        name_domain = email.split('@')
        if len(name_domain) != 2:
            return False

        name, domain = name_domain
        if not name or not domain:
            return False

        server_com = domain.rsplit('.', 1)
        if len(server_com) != 2:
            return False

        server, com = server_com
        if not server or not com:
            return False

        return True


    @staticmethod
    def random_sample_from_email(email, var, expect_to_zero, label):
        sample = { #'email':email, 
            'del':randint(0,1), 'perm':randint(0,1), 'var':var, 'expect_to_zero':expect_to_zero, 'label':label}

        val = sample[var]
        sample['expected_val'] = 0 if expect_to_zero else val

        say("{:30} label {} del {} perm {} var {} expect_to_zero {}".format(email, sample['label'], sample['del'], sample['perm'], sample['var'], sample['expect_to_zero']))

        return sample


    def sample(self, email_list, var, expect_to_zero, label):
        d = dict([(email, self.random_sample_from_email(email, var, expect_to_zero, label)) for email in email_list])
        say()
        self.samples.update(d)


    def sample_from_undel(self, cond):
        emaillist = []
        rows = self.emaildc.select('undeliverable', Lit(cond), 'email', group_by='email', top=self.SAMPLE_SIZE)
        say(self.emaildc.query)

        for email in rows:
            if self.validemail(email):
                emaillist.append(email)

        return emaillist


    def sample_from_perm(self, cond):
        emaillist = []
        rows = self.emaildc.select('remove', Lit(cond), 'email', group_by='email', top=self.SAMPLE_SIZE)
        say(self.emaildc.query)

        for email in rows:
            if self.validemail(email):
                emaillist.append(email)
        return emaillist


    def get_dels(self):
        """
        -- NOT UNDEL
        select count(distinct email) from undeliverable where flags & 0x80 = 0 AND database_id<>593
        
        -- global block (not undel)
        select count(distinct email) from undeliverable where flags & 0x80 > 0 AND flags & 1>0 --0
        
        -- database non-global block (not undel)
        select count(distinct email) from undeliverable where flags & 0x80 = 0 AND database_id=593 AND flags & 1>0 --175231
        """
        say("get_dels")
        var = 'del'
        expect_to_zero = False

        self.sample(self.FAKE_EMAILS[:], var,  expect_to_zero, 'FAKE')
        self.sample(self.sample_from_undel('''
        flags & 0x80 = 0 AND database_id<>{0} 
        and email not in 
        (select email from undeliverable where (flags & 0x80 > 0 or database_id={0}) and flags&0x1=0)'''.format(self.cdb_id)), 
                    var, expect_to_zero, "NOT_OUR_UNDELS")

        # no records in here, just searches entire database in vain:
        # self.sample(self.sample_from_undel('flags & 0x80 > 0 AND flags & 1>0'), var, expect_to_zero, 'glob_blocks')

        self.sample(self.sample_from_undel('''
        flags & 0x80 = 0 AND database_id={0} AND flags & 1>0
        and email not in 
        (select email from undeliverable where (flags & 0x80 > 0 or database_id={0}) and flags&0x1=0)
        '''.format(self.cdb_id)), 
                    var, expect_to_zero, 'CDB_BLOCKS')

        say("get_dels", len(self.samples))



    def get_undels(self):
        """
        -- global undel (and nonblock)
        select count(distinct email) from undeliverable where flags & 0x80 > 0 AND flags & 1=0 -- 18257741

        -- database non-global undel (and nonblock)
        select count(distinct email) from undeliverable where flags & 0x80 = 0 AND database_id=593 AND flags & 1=0 --197129
        """
        say("get_undels")
        var = 'del'
        expect_to_zero = True
        self.sample(self.BAD_EMAILS[:], var,  expect_to_zero, 'BAD')
        self.sample(self.sample_from_undel('flags & 0x80 > 0 AND flags & 1=0'), var, expect_to_zero, 'GLOBAL_UNDELS')
        self.sample(self.sample_from_undel('database_id={} AND flags & 1=0 '.format(self.cdb_id)), var, expect_to_zero, 'LOCAL_UNDELS')
        
        say("get_undels", len(self.samples))
        

 
    def get_removes(self): # not perms
        """
        -- global removes
        SELECT count(*) from remove WHERE  flags & 0x1 > 0 -- 56,612,086
        SELECT top 1000 email from remove WHERE flags & 0x1 > 0 group by email

        -- local non-global removes
        SELECT count(distinct email) from remove WHERE  flags & 0x1 = 0 AND database_id = 593 --2932
        """
        say("get_removes")

        var = 'perm'
        expect_to_zero = True
        self.sample(self.BAD_EMAILS[:], var, expect_to_zero, 'BAD')
        self.sample(self.sample_from_perm('flags & 0x1 > 0'), var, expect_to_zero, 'GLOBAL_REMOVE')
        self.sample(self.sample_from_perm('flags & 0x1 = 0 AND database_id = {}'.format(self.cdb_id)), var, expect_to_zero, 'LOCAL_REMOVE')

        say("get_removes", len(self.samples))


    def get_perms(self):
        """
        -- non-removes
        SELECT count(*) from remove WHERE flags & 0x1 = 0 AND database_id <> 593 --22331582
        """
        say("get_perms")
        var = 'perm'
        expect_to_zero = False
        self.sample(self.FAKE_EMAILS[:], var, expect_to_zero, 'FAKE')

        self.sample(self.sample_from_perm('''
        flags & 0x1 = 0 AND database_id <> {0}
        and email not in 
        (select email from remove where flags & 0x1 > 0 or database_id={0})'''.format(self.cdb_id)), 
                    var, expect_to_zero, 'NOT_OUR_REMOVE')

        say("get_perms", len(self.samples))



    def save_as_dbf(self):
        say('save_as_dbf')
        try:
            os.remove(self.foxtable)
        except FileNotFoundError:
            pass
        self.foxprogram = os.path.join(self.cwd, 'removes_undels_tab_to_dbf.PRG')

        foxpro_progtam = r"""
        CREATE TABLE {} ;
        (email c(65),del c(1),perm c(1))

        Appe From {} Type Deli With Tab
        Use
        * To run, copy this to command window:  do {}
        """.format(self.foxtable, self.dump_sample, self.foxprogram)
        
        with open(self.foxprogram, 'w') as fprg:
            fprg.write(foxpro_progtam)
            
        subprocess.call("start " + self.foxprogram, shell=True)
        # after finish our sample is in sampledump.DBF
        
        input("Press Enter to continue test...")


        
    def dump_samples(self):
        say('dump_samples')
        self.dump_sample = os.path.join(self.cwd, 'sampledump.txt')

        with open(self.dump_sample, 'w') as df:
            for email, sample in self.samples.items():
                df.write("{}\t{}\t{}\n".format(email, sample['del'], sample['perm']))


    def call_tested_program(self):
        say('call_tested_program')
        self.tested_program = 'mark_removes_undels.py'
        self.tested_dest = os.path.join(self.cwd, 'test_output.txt')

        say("subprocess call {}".format(
            ['python', self.tested_program, str(self.campaign_id), self.foxtable, self.tested_dest]
        ))

        subprocess.call(['python', self.tested_program, str(self.campaign_id), self.foxtable, self.tested_dest], shell=True)


    def load_test_results(self):
        say('load_test_results')
        self.test_results = []

        with open(self.tested_dest) as tr:
            for line in tr:
                email, dell, perm = line[:-1].split('\t')
                res = {'email':email, 'del': int(dell), 'perm':int(perm)}
                self.test_results.append(res)
            
        ssay("test results", self.test_results)

    

    def setUp(self):
        self.metacon = Dbcon()
        self.emaildc = Dbcon("EMAILDBSERV2K", "EMAIL")
        self.cwd = os.getcwd()

        self.rerun = False
        if len(sys.argv) > 1 and sys.argv[1]:
            self.rerun = True
            self.cdb_id = int(sys.argv[1])
        else:
            self.get_cdb()
        self.get_campaign_id()


        self.foxtable = os.path.join(self.cwd, 'sampledump.DBF')


        if not self.rerun:
            self.samples = {}
            self.get_dels()
            self.get_undels()
            self.get_removes()
            self.get_perms()


            for i in range(3):
                randemail = list( self.samples.keys() ) [ randint(0, len(self.samples.keys())-1) ]
                ssay('sample {}'.format(i), self.samples[randemail])

            self.dump_samples()
            self.save_as_dbf()


        self.call_tested_program()
        self.load_test_results()


    def test_sample(self):
        for res in self.test_results:
            email = res['email']
            sample = self.samples.get(email, None)
            if not sample:
                say("email -{}- was not in samples".format(email))
                continue

            var = sample['var']
            say("{:30} {:10} col {} = {} {:3} ==> {}".format(email, sample['label'], var, sample[var], 
                                                             '=>0' if sample['expect_to_zero'] else '', res[var]))

            
            if (sample['expect_to_zero'] and res[var] != 0) or (not sample['expect_to_zero'] and res[var] != sample[var]):
                say("sample", sample)
                say("result", res)
                say("\tExpected-0 {} got {}".format('YES' if sample['expect_to_zero'] else '', res[var]))
                say()


if __name__ == '__main__':
    unittest.main()

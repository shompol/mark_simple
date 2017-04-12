#!/usr/bin/env python
try:
    import os
    os.environ.pop('TZ', None)  # call this first before some library somewhere used datetime with bad windoz timezone
    import datetime
    from dbfread import DBF, FieldParser
    import subprocess
    import argparse
    from socket import gethostname
    import sys
    libpath = os.path.join(os.path.dirname(sys.argv[0]), "mark_prequal")
    sys.path.append(libpath)
    import aig
    from dbcon import TolerantDbcon, NotFound
    #from meta import Clientdb
    from oql import EQ, CreateTable, ColDef, DropTable
    from goodtimes import GoodTimes
    import re

    BLK_INSERT_PATH = r'\\listserv2k\d$\data'
    system_stderr = sys.stderr
except Exception as e:
    print("\nTHERE WAS AN ERROR PROCESSING YOUR REQUEST:")
    print(e)
    input("Press Enter to exit...")
    raise


def say(*objs, end='\n'):
    print(*objs, file=sys.stderr, end=end)
    sys.stderr.flush()
    if sys.stderr != system_stderr:
        print(*objs, file=system_stderr, end=end)
        system_stderr.flush()


def get_db_id(campaign_id):
    metadc = TolerantDbcon()
    MAX_ATTEMPTS = 13
    for _ in range(MAX_ATTEMPTS):
        try:
            db_id = metadc.select_one(['ecampaign', 'order_'], EQ(campaign_id=campaign_id) ,'order_.database_id')
            return db_id

        except NotFound:
            say("\nCampaign ID {} Not Found!".format(campaign_id))
            s_campaign_id = input("Please enter a valid Campaign ID ([Enter] to abort): ")
            if not s_campaign_id:
                raise Exception("User chose not to enter a Campaign ID. Terminating...")
            try:
                campaign_id = int(s_campaign_id)
            except ValueError:
                say('"{}" is not a number. Try again.'.format(s_campaign_id))

    raise Exception("Failed after {} promts. Terminating...".format(MAX_ATTEMPTS))


def rename_prev_destination_file(dest):
    # rename it just so we don't accidentally reuse a previous file
    if os.path.isfile(dest):
        try:
            os.remove(dest + '~')
        except FileNotFoundError:
            pass
        os.rename(dest,  dest + '~')


def get_uniq_label():
    # weed out bad chars like '-' from hostname so we can use this label for temp table name
    return '_'.join(re.split(r'\W', gethostname()) + [datetime.datetime.now().strftime('%Y%m%d_%H%M%S')])


def redirect_stderr_to_file(uniqlabel):
    logfile = os.path.join(os.path.dirname(__file__), 'log_' + os.path.basename(__file__)[:-3] + '_' + uniqlabel + '.log')
    sys.stderr = open(logfile, 'w', encoding='utf-8')
    say(__file__, 'started. Logfile:', logfile)


    
class BadValException(Exception):
    pass



class FileldParserErrorNullifier(FieldParser):
    def parseC(self, field, data):
        """Parse char field and return string"""
        return str(data.rstrip(b'\0 '), self.encoding, errors='replace')



class SpeedyMarker:

    def __init__(self, db_id, src, dest, uniqlabel):
        self.db_id, self.dbfpath, self.report_path, self.uniqlabel = db_id, src, dest, uniqlabel
        self.get_time_estimate()

        self.bulkinsert_full_path = self.get_bulkinsert_path()

        self.temp_table = 'temp_del_perm' + self.uniqlabel
        self.srv = "EMAILDBSERV2K"
        self.db = "EMAIL"
    

    def get_bulkinsert_path(self):
        blk_ins_file = 'blk_ins_{}.txt'.format(self.uniqlabel)
        output_path = os.path.dirname(self.report_path)
        return os.path.join(output_path, blk_ins_file)


    def get_time_estimate(self):
        self.src_size = os.path.getsize(self.dbfpath)
        speed_per_MB = 1061471  # bytes per sec
        say("Total ETA for {} MB: {} min".format(self.src_size/1000000, self.src_size / speed_per_MB / 60))


    @staticmethod
    def bit(r, col):
        val = r[col].strip()
        if val == '1':
            return 1
        if val == '0':
            return 0

        raise BadValException("Invalid {} -{}-".format(col, r[col]))
        

    def dump_emails(self):
        say("\nDump emails to '{}'".format(self.bulkinsert_full_path))
        self.n = 0
        with open(self.bulkinsert_full_path, 'w', encoding='CP1252', errors='replace') as blk_ins_fo:
            for self.n,r in enumerate(DBF(self.dbfpath, parserclass=FileldParserErrorNullifier), 1):
                try:
                    email = r['EMAIL'].strip()
                    if not email:
                        continue
                    vperm = self.bit(r, 'PERM')
                    vdel = self.bit(r, 'DEL')
                    blk_ins_fo.write("{}\t{}\t{}\n".format(email, vdel, vperm))

                except BadValException as e:
                    say("EMAIL {} error {}".format(email, e))
                    continue
               
        self.gt.stamp("Write blk ins file: Wrote {} recs".format(self.n))


    def mark(self, permdel, tbl, global_flag, blocked_cond=None):
        speed_per_10000 = 38  # secs
        eta = speed_per_10000 * (self.n)/10000
        say("\nMark {}, ETA: {:.2f} mins".format(permdel, eta/60))

        tbl_cond = 'flags & {flag} > 0 OR database_id = {dbid}'.format(flag=global_flag, dbid=self.db_id)
        if blocked_cond:
            tbl_cond = ' AND '.join([blocked_cond, tbl_cond.join("()")])

        sql = """
        UPDATE {temp_table}
        SET {permdel} = 0
        FROM {temp_table} t
        JOIN (
            SELECT email from {tbl}
            WHERE  {tbl_cond}
            GROUP BY email
        ) r
        ON t.email = r.email
        WHERE t.{permdel} = 1
        """.format(temp_table=self.temp_table, permdel=permdel, tbl=tbl, tbl_cond=tbl_cond)
        say(sql)

        self.emaildc.execute(sql)
        self.emaildc.commit()
        self.gt.stamp("Mark {} Done.".format(permdel))


    def mark_del_perm(self):
        with GoodTimes() as self.gt:
            self.dump_emails()
            self.emaildc = TolerantDbcon(self.srv, self.db)
            self.blk_ins()
            
            self.mark(permdel='perm', tbl='remove', global_flag='0x1')
            self.mark(permdel='del', tbl='undeliverable', global_flag='0x80', blocked_cond = 'flags & 1=0')

            self.bcp_result()
            self.emaildc(DropTable(self.temp_table))
            os.remove(self.bulkinsert_full_path)
        say('Done')


    def bcp_result(self):
        say("""Writing result file with columns "Email DEL PERM" to '{}'""".format(self.report_path))
        bcpsql = 'select email, del, perm from email.dbo.{}'.format(self.temp_table)
        cmd = ['bcp', bcpsql, 'QUERYOUT', self.report_path, '-c', '-t\t', '-U', aig.uid, '-P', aig.pwd, '-S', 'emaildbserv2k']
        # Never show this say(' '.join(cmd))        
        try:
            bcp_outp = subprocess.check_output(cmd, shell=True)
        except:
            say('BCP ERROR:', bcp_outp)
            raise

    #bcp inventory.dbo.fruits in "C:\fruit\inventory.txt" -c -T 
    def blk_ins(self):
        say("\nBCP INSERT '{}' into {}".format(self.bulkinsert_full_path, self.temp_table))
        self.emaildc(CreateTable(self.temp_table,
                                 [ 
                                     ColDef('email', 'varchar', 100, null=False),
                                     ColDef('del', 'BIT', null=False),
                                     ColDef('perm', 'BIT', null=False)
                                 ]))

        say(self.emaildc)
        cmd = ['bcp', 'email.dbo.{}'.format(self.temp_table), 'IN', self.bulkinsert_full_path, '-c', '-t\t', '-U', aig.uid, '-P', aig.pwd, '-S', 'emaildbserv2k']
        # Never show this say(' '.join(cmd))        
        try:
            subprocess.check_output(cmd, shell=True)

        except:
            say('BCP ERROR')
            raise

        #sql = "BULK INSERT {} FROM '{}'".format(self.temp_table, self.bulkinsert_full_path)
        #say(sql)
        #self.emaildc.execute(sql)
        #self.emaildc.commit()
        self.gt.stamp("BCP INSERT Done.")

    @staticmethod
    def get_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("campaign_id", type=int, help="Campaign ID used to determine DB#")
        parser.add_argument("src",  help="Source DBF file, must have custno column")
        parser.add_argument("dest", help="Destination .txt file to generate TAB-separated")
        args = parser.parse_args()
        
        if not os.path.isfile(args.src):
            raise Exception("File not found:", args.src)

        output_dir = os.path.dirname(args.dest)
        if output_dir and not os.path.exists(output_dir):
            raise Exception("Ouput dir not found:", args.dest)

        db_id = get_db_id(args.campaign_id)
        say("Args: clientdb {} src: {} dest: {}".format(db_id, args.src, args.dest))
        return db_id, args.src, args.dest
        
    @classmethod
    def main(cls):
        try:
            uniqlabel = get_uniq_label()
            db_id, src, dest = cls.get_args()
            redirect_stderr_to_file(uniqlabel)
            rename_prev_destination_file(dest)
            s = cls(db_id, src, dest, uniqlabel)
            s.mark_prequal()
            say("Please collect your tab-separated table at '{}'".format(s.report_path))
        except Exception as e:
            print("\nTHERE WAS AN ERROR PROCESSING YOUR REQUEST:")
            print(e)
            input("Press Enter to exit...")
        except:   # pylint: disable=broad-except
            print("\nTHERE WAS AN ERROR PROCESSING YOUR REQUEST")
            input("Press Enter to exit...")

            
if __name__ == '__main__':
    SpeedyMarker.main()

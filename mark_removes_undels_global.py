#!/usr/bin/env python
import argparse
import os
from mark_removes_undels import SpeedyMarker, say


class SpeedyMarkerGlobal(SpeedyMarker):
    @staticmethod
    def get_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("src",  help="Source DBF file, must have EMAIL column")
        parser.add_argument("dest", help="Destination .txt file to generate TAB-separated \"Email DEL PERM\"")
        args = parser.parse_args()

        if not os.path.isfile(args.src):
            raise Exception("File not found:", args.src)

        output_dir = os.path.dirname(args.dest)
        if output_dir and not os.path.exists(output_dir):
            raise Exception("Ouput dir not found:", args.dest)

        say("Args: src: {} dest: {}".format(args.src, args.dest))
        return -1, args.src, args.dest  # -1 stands for db_id which we dont need in global
    
            
    def mark(self, permdel, tbl, global_flag, blocked_cond=None):  # global_flag as well as self.db_id is unused in global version
        speed_per_10000 = 38  # secs
        eta = speed_per_10000 * (self.n)/10000
        say("\nMark {}, ETA: {:.2f} mins".format(permdel, eta/60))
        tbl_cond = " WHERE {}".format(blocked_cond) if blocked_cond else " WHERE flags & 0x400 = 0"
        sql = """
        UPDATE {temp_table}
        SET {permdel} = 0
        FROM {temp_table} t
        JOIN (
            SELECT email from {tbl} {tbl_cond}
            GROUP BY email
        ) r
        ON t.email = r.email
        WHERE t.{permdel} = 1
        """.format(temp_table=self.temp_table, permdel=permdel, tbl=tbl, tbl_cond=tbl_cond)
        say(sql)

        self.emaildc.execute(sql)
        self.emaildc.commit()
        self.gt.stamp("Mark {} Done.".format(permdel))
    

if __name__ == '__main__':
    SpeedyMarkerGlobal.main()

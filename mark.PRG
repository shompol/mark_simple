parameter m->field, m->filename

set proc to z:\foxdata\aigproc

set talk off
set safety off


?'Started ' + time(0)



dumpfile='c:\foxdata\temp\mark_'+alltr(str(rand()*100000)) +'.csv'
?'Using dumpfile: ' + dumpfile

use (m->filename)
copy field custno to (dumpfile) sdf
use

?'Dumped to csv ' + time(0)

*__cmd='!python c:\home\dbf\mark.py '                + (m->field) + ' ' + (dumpfile)
__cmd= '!python c:\foxdata\foxwrap.py python c:\foxdata\mark\mark.py ' + (m->field) + ' ' + (dumpfile)
?__cmd
&__cmd
?'Populated values ' + time(0)

   do ModiStru with (m->filename), "", "8", "C", "advhyg"
   do ModiStru with (m->filename), "advhyg", "8", "C", ""
   do ModiStru with (m->filename), "", "2", "C", "country_cd"   



** THIS SECTION NEEDS TO HAVE PARAMETER "column" BUT I DON't KNOW HOW TO DO THIS IN FOXPRO, SO TWO COPIES! -BZ
m->field = lower(m->field)

if m->field = 'h' or m->field = 'hyg' or m->field = 'hygiene' or m->field = 'advhyg' 

*if m->field = 'h' or m->field = 'hyg' or m->field = 'hygiene'

   m_temp=juststem((dumpfile))
   create table (m_temp) (custno c(16), hygiene c(3), hygiene2 c(3), country_cd c(2))

   use (m_temp)
   appe from (dumpfile) type deli with tab
   use
   erase (dumpfile)
   ?'Populated Temp table: ' + m_temp

   *do ModiStru with (m->filename), "", "3", "C", "hygiene"
   *do ModiStru with (m->filename), "", "3", "C", "hygiene2"
   *do ModiStru with (m->filename), "", "2", "C", "country_cd"   
   ** alter table (m->filename) add column hygiene c(3) -- replaced by modistru

   select 2
   use (m_temp)
   index on alltr(custno) to t
   select 1
   use (m->filename)
   set relation to alltr(custno) into b
       repl all advhyg with iif(found(2) and !empty(b->hygiene), alltr(b->hygiene)+'#',advhyg),;
                  advhyg with iif(found(2) and !empty(b->hygiene2) and !alltr(b->hygiene2)$advhyg, alltr(advhyg)+alltr(b->hygiene2)+'#',advhyg),;
                  country_cd with iif(found(2),b->country_cd,country_cd)
*       repl all hygiene with b->hygiene,;
                hygiene2 with b->hygiene2,;
                country_cd with b->country_cd for found(2)
       use
       select 2
       use

else


   m_temp=juststem((dumpfile))
   create table (m_temp) (custno c(16), fa c(3))

   use (m_temp)
   appe from (dumpfile) type deli with tab
   use
   erase (dumpfile)
   ?'Populated Temp table: ' + m_temp

   ** alter table (m->filename) add column fa c(3)
   do ModiStru with (m->filename), "", "2", "C", "fa"

   select 2
   use (m_temp)
   index on alltr(custno) to t
   select 1
   use (m->filename)
   set relation to alltr(custno) into b
       repl all fa with b->fa  for found(2)
       use
       select 2
       use

endif

erase (m_temp)+'.dbf'
set safety on
set talk on

?'Time Finished ' + time()

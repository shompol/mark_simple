Parameter m->filename

set proc to z:\foxdata\aigproc


m->filename  = CUTEXT(UPPER(ALLTRIM(m->filename)))+".DBF"

?'Time Started ' + time()

*                                                                                        Input file               Output File                       
__cmd = '!python \\fileserv2k\z-drive\Python\tools\dbf\mark_removes_undels_global.py ' + m->filename + ' c:\foxdata\m_r_u_output.txt '
&__cmd

Do ModiStru with (m->filename), "", "1", "C", "del",;
                                "", "1", "C", "perm"

Set Safety Off
Set Talk Off
Use (m->filename)
Copy Struct Fields email,del,perm To c:\foxdata\m_r_u_output.Dbf
Use

Use c:\foxdata\m_r_u_output.Dbf
Appe From c:\foxdata\m_r_u_output.txt Type Deli With Tab
Use

Select 2
Use c:\foxdata\m_r_u_output.Dbf
Index On left(Upper(email),100) To T Uniq For !Empty(email)
Select 1
Use (m->filename)
Set Relation To  left(Upper(email),100) Into b
Repl All del With iif(b->del='0' and del ='1',b->del,del),;
             perm With iif(b->perm='0' and perm ='1',b->perm,perm)  For Found(2)
Use
Select 2
Use
Set Safety On
Set Talk On

?'Time Finished ' + time()

## @namespace pub_smtp
#  @ingroup   pub_util
#  @brief Very simple email sender
#  @details A simple SMTP function pub_smtp sends an email message

# python import
import sys,smtplib,os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ds_util import
from pub_env import kSMTP_ACCT, kSMTP_SRVR, kSMTP_PASS

## @function
#  @brief Function 
def pub_smtp(sender=kSMTP_ACCT, smtp_domain=kSMTP_SRVR, passwd=kSMTP_PASS,
             receiver=None, subject=None, text=None):

    if not sender:
        sys.stderr.write('\033[95mNo email sender account set! Aborting...\033[00m\n')
        return

    if not smtp_domain:
        sys.stderr.write('\033[95mSMTP domain set! Aborting...\033[00m\n')
        return

    if not passwd:
        sys.stderr.write('\033[95mNo email password set! Aborting...\033[00m\n')
        return

    msg=MIMEMultipart('alternative')
    msg['Subject']=subject
#    msg['From']=sender
    msg['To']=", ".join(receiver)
    if text.find('<html>')>=0:
        msg.attach(MIMEText(text,'html'))
    else:
        msg.attach(MIMEText(text,'plain'))
    server = smtplib.SMTP(smtp_domain)
    server.ehlo()
    server.starttls()
    server.login('drinkingkazu.pubs',"pubs.drinkingkazu")
    server.sendmail(sender,receiver,msg.as_string())
    server.quit()

if __name__=="__main__":

    if not len(sys.argv)==4:
        print "usage: pub_smtp.py $RECEIVER $SUBJECT $TEXT"
        sys.exit(1)
    if os.path.isfile(sys.argv[3]):
        pub_smtp( receiver = sys.argv[1].split(None),
                  subject  = sys.argv[2],
                  text     = open(sys.argv[3],'r').read() )
    else:
        pub_smtp( receiver = sys.argv[1].split(None),
                  subject  = sys.argv[2],
                  text     = sys.argv[3] )
        

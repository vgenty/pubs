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
from pub_exception import BaseException;
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
    recipients = receiver.strip().split(',')
    msg['To']=", ".join( recipients )

    if text.find('<html>')>=0:
        msg.attach(MIMEText(text,'html'))
    else:
        msg.attach(MIMEText(text,'plain'))
    try:
        server = smtplib.SMTP(smtp_domain)
    except Exception as e:
        raise BaseException("SMTP conn. failure (check domain info)! Email cannot be sent...")
    try:
        server.ehlo()
        server.starttls()
        server.login( sender, passwd )
    except Exception as e:
        raise BaseException("SMTP login failure (check login info)! Email cannot be sent...")
    try:
        server.sendmail(sender, recipients, msg.as_string())
        server.quit()
    except smtplib.SMTPRecipientsRefused as e:
        msg=''
        for x in e.recipients.keys():
            msg += 'Recipient: %s...\n' % x
            msg += e.recipients[x][1]
        raise BaseException(msg)
    except smtplib.SMTPSenderRefused as e:
        raise BaseException('SMTP sender auth. failure! Email cannot be sent...')
    except smtplib.SMTP as e:
        raise BaseException('SMTP UNKNOWN ERROR! Email cannot be sent...')
    
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
        

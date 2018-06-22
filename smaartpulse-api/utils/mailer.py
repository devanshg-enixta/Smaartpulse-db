import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email.mime.application import MIMEApplication
from os.path import basename


def send_mail(send_to, subject, text, user='smaartalert@enixta.com', passwd='lbnuybiinkyvsrqx', files=None,
              **kwargs):
    assert isinstance(send_to, list)

    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)

    if 'type' in kwargs:
        msg['Subject'] = '[{}] '.format(kwargs.get('type'))+str(subject)
    else:
        msg['Subject'] = subject

    msg.attach(MIMEText(text, 'html'))
    print 'Started adding file'
    for f in files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(f)
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
            msg.attach(part)


    smtpserver = smtplib.SMTP("smtp.gmail.com:587")
    # smtpserver.set_debuglevel(False)
    smtpserver.ehlo()
    smtpserver.starttls()
    # smtpserver.ehlo
    smtpserver.login(user, passwd)
    print 'Logged in'
    smtpserver.sendmail(user, send_to, msg.as_string())
    print 'sent mail'
    smtpserver.close()
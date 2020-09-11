import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from private.keys import MAIL_PASSWORD, MAIL_ADDRESS


def send_email(recipient, message_, subject="This is TEST", message_type='html'):
    msg = MIMEMultipart()  # create a message
    # setup the parameters of the message
    # msg['From'] = MY_ADDRESS
    msg['From'] = str(Header(f'bot mail <{MAIL_ADDRESS}>'))
    msg['To'] = recipient
    msg['Subject'] = subject
    # add in the message body
    msg.attach(MIMEText(message_, message_type))
    # send
    with smtplib.SMTP_SSL(host='mail.eprojecttrackers.com', port=465) as s:
        s.login(MAIL_ADDRESS, MAIL_PASSWORD)
        s.send_message(msg, MAIL_ADDRESS, recipient)



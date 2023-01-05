import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
import os
import json


# Set Status of File
Stage = os.environ.get("PYENV_HOME")

# Open config.json file and load data
with open("config.json") as json_data_file:
    my_config = json.load(json_data_file)

# Determine status and access values for correct config file
if Stage == "DEV":
    config = my_config["uat"]
else:
    config = my_config["prod"]


# Send Error Email when Exceptions
def daily_email(file):
    pwarehouse = f"{config['email-to']}"
    sender = f"{config['email-from']}"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Daily Kittable"
    msg["From"] = f"{config['email-from']}"
    msg["To"] = f"{config['email-to']}"

    filename = file  # In same directory as script

    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition", f"attachment; filename= {filename}",
    )

    # EMail Body

    text = (
        "Here are the stockcodes that are kittable as of the time this email was sent."
    )
    html = """\
    <html>
      <head></head>
      <body>
        <p>Here are the stockcodes that are kittable as of the time this email was sent. </p>

      </body>
    </html>
    """

    body1 = MIMEText(text, "plain")
    body2 = MIMEText(html, "html")

    msg.attach(body1)
    msg.attach(body2)
    msg.attach(part)

    try:

        server = smtplib.SMTP("relay.benchmade.com", 25)
        server.ehlo()
        server.starttls()
        server.set_debuglevel(False)
        server.sendmail(sender, pwarehouse, msg.as_string())
        server.quit()

    except ConnectionError as e:
        print(e)


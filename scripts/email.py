
from airflow.models import Variable
import smtplib


def send_dag_email(**context):
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()

        server.login(Variable.get("gmail_email"),
                     Variable.get("gmail_password"))

        subject = f"Reporte {context['dag']} {context['ds']}"

        body_text = f"Tarea : {context['task_instance_key_str']}\n\n"

        message = f"Subject: {subject}\n\n{body_text}"

        server.sendmail(Variable.get("gmail_email"),
                        Variable.get("gmail_email"), message)

        print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")


def send_price_alert(subject):
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()

        server.login(Variable.get("gmail_email"),
                     Variable.get("gmail_password"))

        body_text = f"Price alert\n\n"

        message = f"Subject: {subject}\n\n{body_text}"

        server.sendmail(Variable.get("gmail_email"),
                        Variable.get("gmail_email"), message)

        print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")
        raise e

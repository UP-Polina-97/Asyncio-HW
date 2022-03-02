import sqlite3
from sqlite3 import Error
import datetime
import aiosmtplib
from email.message import EmailMessage
import asyncio
from more_itertools import chunked


def connection_create(db_file_):
    connection = None
    print(connection)
    try:
        connection = sqlite3.connect(db_file_)
    except Error as er:
        print(er)
    return connection


async def contacts_send(contact: tuple):
    email_message = EmailMessage()
    email_message["From"] = "root@localhost"
    email_message["To"] = contact[3]
    email_message["Subject"] = "Hello"
    email_message.set_content(f"Dear {contact[1]} {contact[2]}\nthank you, for using our adds services.")
    await aiosmtplib.send(email_message, hostname="smtp.mail.ru", port=465, use_tls=True, username="email", password="password")
    print("Succes you did it!", str(datetime.datetime.now()))


async def main(): 
    db_file = 'contacts.db'
    connect = connection_create(db_file)
    cursor = connect.cursor()
    cursor.execute("SELECT * FROM contacts")
    contacts = cursor.fetchall()
    for chunk in chunked(contacts, 10):
        print(list(chunk))
        tasks = [asyncio.create_task(contacts_send(contact)) for contact in chunk]
        results = await asyncio.gather(*tasks)
        print(results)
    cursor.close()

#event_loop = asyncio.get_event_loop()
event_loop = asyncio.new_event_loop()
asyncio.set_event_loop(event_loop)
event_loop.run_until_complete(main())
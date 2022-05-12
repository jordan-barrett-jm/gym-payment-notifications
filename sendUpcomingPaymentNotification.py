#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
Process:

1. Check to see if any of the paid dates in the main sheet differ from the previously paid dates in the secondary sheet
2. If the secondary sheet has not been populated, populate it with current payment schedule info
3. If it is populated and there is no difference, do nothing
4. If it is populated and there is a difference, update the Previous Paid Date column in the secondary sheet to the paid date value in the main sheet and change Notified to No
5. Check for customers who have not been notified and have a due date within 3 days
6. For all customers who meet this criteria, notify the gym owner and the customer if an email is present for them

"""


# In[ ]:


import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import datetime as dt
from mailjet_rest import Client
import os
import numpy as np
from os.path import join, dirname
from dotenv import load_dotenv


# In[ ]:


#set up logging
import logging

logging.basicConfig(filename='app.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


# In[ ]:


#load .env file
load_dotenv()
#set global variables
OWNER_EMAIL = os.environ.get("OWNER_EMAIL")
MAILJET_KEY = os.environ.get("MAILJET_KEY")
MAILJET_SECRET = os.environ.get("MAILJET_SECRET")
GOOGLE_SHEETS_KEY = os.environ.get("GOOGLE_SHEETS_KEY")


# In[ ]:


# define the scope
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

# add credentials to the account
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_KEY, scope)

# authorize the clientsheet 
client = gspread.authorize(creds)


# In[ ]:


try:
    sheet = client.open('customer sheets')
    logging.info("Connected to Google sheets...")
except Exception as e:
    logging.error(str(e))
    raise


# In[ ]:


current_schedule = sheet.get_worksheet(0)
previous_schedule = sheet.get_worksheet(1)


# In[ ]:


#convert to dataframe
current_schedule_df = pd.DataFrame.from_dict(current_schedule.get_all_records())
previous_schedule_df = pd.DataFrame.from_dict(previous_schedule.get_all_records())


# In[ ]:


#create a new secondary sheet which will reflect new dates paid
def updateScheduleDataframe(customers, previous_schedule_df, current_schedule_df):
    new_df = previous_schedule_df
    for customer in customers:
        customer_exists = customer in previous_schedule_df["Customer Name"].values
        current_date_paid = current_schedule_df[current_schedule_df["Customer Name"] == customer]["Date Paid"].values[0]
        if customer_exists:
            previous_date_paid = previous_schedule_df[previous_schedule_df["Customer Name"] == customer]["Previous Paid Date"].values[0]
            if current_date_paid == previous_date_paid:
                pass
            else:
                customer_idx = np.where(updated_schedule_df["Customer Name"].values == customer)[0][0]
                new_df.at[customer_idx, "Previous Paid Date"] = current_date_paid
                new_df.at[customer_idx, "Notified"] = "N"
        else:
            new_row = {
                'Customer Name': customer,
                'Previous Paid Date': current_date_paid,
                'Notified': 'N'
            }
            new_df = new_df.append(new_row, ignore_index=True)
    return new_df
customers = current_schedule_df["Customer Name"].values
updated_schedule_df = updateScheduleDataframe(customers, previous_schedule_df, current_schedule_df)


# In[ ]:


active_customers_df = current_schedule_df[current_schedule_df["Date Paid"] != ""]


# In[ ]:


#put all due customers into an array
due_customers = []
for customer in active_customers_df["Customer Name"]:
    date_paid = active_customers_df[active_customers_df["Customer Name"] == customer]["Date Paid"].values[0]
    #handle any invalid values in the Date Paid column
    try:
        date_paid_dt = dt.datetime.strptime(date_paid, "%d.%m.%Y")
    except Exception as e:
        logging.error(str(e))
        pass
    #find due date - date paid + 28 days
    date_due_dt = date_paid_dt + dt.timedelta(days=28)
    #if due date is within 3 days of right now then add customer to due list
    payment_due = (date_due_dt - dt.timedelta(days=3)) <= dt.datetime.now()
    #find if a notification was already sent for this customer for this payment schedule
    already_notified = customer in updated_schedule_df[updated_schedule_df["Notified"] == "Y"]["Customer Name"].values
    if (not already_notified) and payment_due:
        due_customers.append(customer)


# In[ ]:


logging.info(f"Customers with due payments: {due_customers}")


# In[ ]:


due_customers_df = active_customers_df[active_customers_df["Customer Name"].isin(due_customers)]


# In[ ]:


def notifyOwner(customer_name, customer_duedate):
    logging.info(f"Gym owner has been notified of upcoming payment for {customer_name}")
    mailjet = Client(auth=(MAILJET_KEY, MAILJET_SECRET), version='v3.1')
    data = {
      'Messages': [
        {
          "From": {
            "Email": "evolutionzgymnotifications@gmail.com",
            "Name": "Evolutionz Gym"
          },
          "To": [
            {
              "Email": OWNER_EMAIL,
              "Name": "Owner"
            }
          ],
          "Subject": f"Payment due for {customer_name} on {customer_duedate}",
          "HTMLPart": f"<h3>Upcoming Payment Notification</h3><br />Please note that the gym payment for {customer_name} should be paid on {customer_duedate}",
          "CustomID": "PaymentNotification"
        }
      ]
    }
    result = mailjet.send.create(data=data)
    print (result.status_code)
    print (result.json())


# In[ ]:


def updateSecondarySheet(updated_df):
    previous_schedule.update([updated_df.columns.values.tolist()] + updated_df.values.tolist())
    logging.info("Secondary sheet udpated")


# In[ ]:


def notifyCustomer(customer_email, customer_duedate, customer_name):
    logging.info(f"{customer_name} has been notified of upcoming payment")
    mailjet = Client(auth=(MAILJET_KEY, MAILJET_SECRET), version='v3.1')
    data = {
      'Messages': [
        {
          "From": {
            "Email": "evolutionzgymnotifications@gmail.com",
            "Name": "Evolutionz Gym"
          },
          "To": [
            {
              "Email": customer_email,
              "Name": "Evolutionz Gym Customer"
            }
          ],
          "Subject": f"Evolutionz Gym payment due on {customer_duedate} - {customer_name}",
          "HTMLPart": f"<h3>Upcoming Payment Notification</h3><br />Please note that your payment to Evolutionz Gym is due on {customer_duedate}",
          "CustomID": "PaymentNotification"
        }
      ]
    }
    result = mailjet.send.create(data=data)
    print (result.status_code)
    print (result.json())


# In[ ]:


#notify each customer if there is an email present
#notify the owner
for customer in due_customers:
    customer_df = due_customers_df[due_customers_df["Customer Name"] == customer]
    customer_email = customer_df["Customer Email"].values
    customer_name = customer_df["Customer Name"].values[0]
    customer_duedate_dt = dt.datetime.strptime(customer_df["Date Paid"].values[0], "%d.%m.%Y") + + dt.timedelta(days=28)
    customer_duedate = customer_duedate_dt.strftime(" %B %d, %Y")
    notifyOwner(customer_name, customer_duedate)
    if customer_email:
        notifyCustomer("1699blue@yopmail.com", customer_duedate, customer_name)
    print (customer_name)
    #set the customer "Notified" value to Y
    customer_idx = np.where(updated_schedule_df["Customer Name"].values == customer_name)[0][0]
    updated_schedule_df.at[customer_idx, "Notified"] = "Y"


# In[ ]:


updateSecondarySheet(updated_schedule_df)


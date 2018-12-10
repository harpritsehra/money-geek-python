import plaid
import datetime

def get_client(client_id, public_key, secret, environment):
  client = plaid.Client(client_id=client_id, secret=secret,
    public_key=public_key, environment=environment)
  return client

def get_accounts(client, access_token):
  response = client.Accounts.get(access_token)
  accounts = response['accounts']
  return accounts

def get_transactions(client, access_token, daysAgoStart, daysAgoEnd):
  if daysAgoStart < daysAgoEnd:
    raise Exception("Days ago start: {} cannot be less than days ago end: {}".format(daysAgoStart, daysAgoEnd))
  start_date = "{:%Y-%m-%d}".format(datetime.datetime.now() + datetime.timedelta(-daysAgoStart))
  end_date = "{:%Y-%m-%d}".format(datetime.datetime.now() + datetime.timedelta(-daysAgoEnd))
  response = client.Transactions.get(access_token, start_date, end_date)
  return response

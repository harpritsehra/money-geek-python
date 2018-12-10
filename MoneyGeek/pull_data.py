import sys
import os
from dao import DataAccessor as DA
import plaid_dao as PDA
import sqlite3
import time

def main():

  PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
  PLAID_SECRET = os.getenv('PLAID_SECRET')
  PLAID_PUBLIC_KEY = os.getenv('PLAID_PUBLIC_KEY')
  PLAID_ENV = os.getenv('PLAID_ENV', 'sandbox')
  if PLAID_CLIENT_ID is None or PLAID_SECRET is None or PLAID_PUBLIC_KEY is None:
    print("ERROR: PLAID env variables are not set!")
    sys.exit(1)

  # read in sqlite3 file
  if len(sys.argv) != 3:
    print("Usage: {} db_file days_ago_start".format(sys.argv[0]))
    sys.exit(1)
  db_file = sys.argv[1]
  days_ago_start = int(sys.argv[2])
  days_ago_end = 0
  if days_ago_start <= days_ago_end:
    print("ERROR: days_ago_start ({}) must be greater than days_ago_end({})".format(days_ago_start, days_ago_end))
  da = DA(db_file)

  client = PDA.get_client(PLAID_CLIENT_ID, PLAID_PUBLIC_KEY, PLAID_SECRET, PLAID_ENV)

  # For each user
  userIds = da.get_all_userID()
  for userRow in userIds:
    print("INFO: Processing user: {}".format(userRow["username"]))
    userConns = da.get_user_connections(userRow["userID"])
    # For each userConn
    for userConn in userConns:
      institutionInfo = da.get_institution_info(userConn["institutionID"])
      print("-> INFO: Processing institution: {}".format(institutionInfo["name"]))
      access_code = userConn["accessCode"]
      try:
        txns = PDA.get_transactions(client, access_code, days_ago_start, days_ago_end)
        num_txns = txns["total_transactions"]
        print("-> INFO: Got {} transactions".format(num_txns, ))
        transactions = txns["transactions"]
        inserted = 0
        skipped_dup = 0
        skipped_pending = 0
        for txn in transactions:
          if not txn["pending"]:
            try:
              category = txn["category"]
              cat = category[0]
              if len(category) > 1:
                subcat = category[1]
              else:
                subcat = None
              da.add_transaction(txn["transaction_id"], txn["account_id"], cat, subcat,
                txn["name"], txn["amount"], txn["date"], None)
              inserted += 1
            except sqlite3.IntegrityError:
              skipped_dup += 1
          else:
            skipped_pending += 1
        print("Summary: Total: {}, Inserted: {}, Skipped Duplicate: {}, Skipped Pending: {}".format(num_txns, inserted, skipped_dup, skipped_pending))
        print("Sleeping for 5 Seconds...")
        time.sleep(5)
      except:
        print("ERROR: Could not get transactions for {}".format(institutionInfo["name"]))

if __name__ == "__main__":
  main()

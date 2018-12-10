import os
import time
import json
import sqlite3
from dao import DataAccessor as DA
import getpass
import plaid_dao as PDA

class ManualUpdater:

  PLAID_CLIENT_ID = os.getenv('PLAID_CLIENT_ID')
  PLAID_SECRET = os.getenv('PLAID_SECRET')
  PLAID_PUBLIC_KEY = os.getenv('PLAID_PUBLIC_KEY')
  PLAID_ENV = os.getenv('PLAID_ENV', 'sandbox')

  def __init__(self, db_file):
    self.da = DA(db_file)
    self.client = PDA.get_client(self.PLAID_CLIENT_ID, self.PLAID_PUBLIC_KEY, self.PLAID_SECRET, self.PLAID_ENV)

  def set_user(self, username):
    self.username = username
    self.userConn = self.get_user_and_connection_info()
    print("INFO: Successfully logged in user: {}".format(self.username))

  def add_user(self):
    username = raw_input("Username?: ")
    email = raw_input("Email?: ")
    password = getpass.getpass()
    print("Re-enter password!")
    password2 = getpass.getpass()
    if password != password2:
      raise(Exception("Passwords did not match!"))
    else:
      self.da.add_user(username, email, password)
      print("INFO: Successfully added new user")

  def add_connection(self):
    if self.username:
      user_info = self.da.get_user_info(self.username, "")
      instID = self.choose_institution()
      ac = raw_input("Access Code?: ")
      self.da.add_connection(user_info["userID"], instID, ac)
    else:
      print("ERROR: Username unknown")
  
  def add_institution(self):
    term = raw_input("Search criteria?: ")
    found = self.find_institution(term)
    institutions = []
    for i in found["institutions"]:
      institutions.append((i["name"], i["institution_id"]))
    self.printTupleList(institutions)
    choice = int(raw_input("InstitutionID?: "))
    try:
      self.da.add_institution(institutions[choice][1], institutions[choice][0])
    except IOError as e:
      print("WARN: Institution not added because -> " + e.message)
  
  def find_institution(self, term):
    response = self.client.Institutions.search(term)
    return response
  
  def get_user_and_connection_info(self):
    if self.username:
      user_info = self.da.get_user_info(self.username)
      all_conn = self.da.get_user_connections(user_info["userID"])
      if len(all_conn) > 0:
        conns = []
        for conn in all_conn:
          conns.append((conn["name"], conn))
        self.printTupleList(conns)
        choice = int(raw_input("Choose institution?: "))
        conn_info = conns[choice][1]
        if conn_info:
          return {"user_info" : user_info, "conn_info" : conn_info}
        else:
          print("ERROR: could not get connection info")
      else:
        print("WARN: No connections setup for this user!")
    return None
  
  def get_plaid_accounts(self):
    if self.userConn:
      user_info = self.userConn["user_info"]
      conn_info = self.userConn["conn_info"]
      access_code = conn_info["accessCode"]
      accounts = PDA.get_accounts(self.client, access_code)
      print("INFO: Got {} accounts".format(len(accounts)))
      insert = raw_input("Insert into database or print? (Y/N/P): ")
      if insert == "Y":
        failures = 0
        for a in accounts:
          try:
            print("Adding: {}: {} ({}, {}) -> {}".format(a["mask"], a["name"], 
              a["type"], a["subtype"], a["balances"]["current"]))
            self.da.add_account(conn_info["connectionID"], a["account_id"], a["mask"], a["name"], 
              a["official_name"], a["type"], a["subtype"])
          except IOError:
            print("ERROR: Could not insert account")
            failures += 1
        if failures > 0:
          print("WARN: failed to insert {} out of {} accounts".format(failures, len(accounts))) 
      elif insert == "P":
        for a in accounts:
          print(a)
          print()
          time.sleep(5)
    else:
      print("ERROR: Could not get either user or connection")
  
  def get_transactions(self):
    if self.userConn:
      user_info = self.userConn["user_info"]
      conn_info = self.userConn["conn_info"]
      access_code = conn_info["accessCode"]
      das = int(raw_input("DaysAgoStart?: "))
      dae = int(raw_input("DaysAgoEnd? (must be less than {}): ".format(das)))  
      txns = PDA.get_transactions(self.client, access_code, das, dae)
      num_txns = txns["total_transactions"]
      print("INFO: Got {} transactions".format(num_txns))
      transactions = txns["transactions"]
      insert = raw_input("Insert into database or Print? (Y/N/P): ")

      if insert == "Y":
        for txn in transactions:
          try:
            category = txn["category"]
            cat = category[0]
            if len(category) > 1:
              subcat = category[1]
            else:
              subcat = None
            self.da.add_transaction(txn["transaction_id"], txn["account_id"], cat, subcat,
              txn["name"], txn["amount"], txn["date"], None)
          except sqlite3.IntegrityError:
            print("INFO: Skipping duplicate transaction")
      elif insert == "P":
        for txn in transactions:
          print(txn)
          time.sleep(5)
      elif insert == "N":
        for txn in transactions:
          print(json.dumps(txn))
      else:
        print("INFO: Not inserting.")
    else:
      print("ERROR: Could not get either user or connection")
      
  def upload_from_file(self):
    filename = raw_input("Enter filename with path: ")
    if self.userConn:
      user_info = self.userConn["user_info"]
      conn_info = self.userConn["conn_info"]
      access_code = conn_info["accessCode"]
      try:
        f = open(filename, "r")
        accountID = self.choose_account()
        counter = 0
        success = 0        
        for line in f:
          counter += 1
          line = line.strip()
          fields = line.split("\t")
          catID = None # Some rows aren't categorised
          if len(fields) > 4:
            userCategory = fields[4]
            categoryInfo = self.da.get_category_info(userCategory)
            if categoryInfo:
              catID = categoryInfo["categoryID"]
            else:
              print("WARN: Unrecognised category: {}, inserting categorID=None instead".format(userCategory))
              catID = None
          else:
            print("DODGY ROW!: {}".format(fields))
          date = self.da.convertDateToSQLDate(fields[0])
          txnID = filename + "@" + str(counter)
          try:
            self.da.add_transaction(txnID, accountID, "ManualUpload", None, fields[2], fields[3], date, catID)
            success += 1
          except sqlite3.IntegrityError:
            print("INFO: Skipping duplicate transaction")
        print("Uploaded {} out of {} transactions".format(success, counter))
        f.close()
      except:
        print("ERROR: Could not process file")
        raise

  def choose_account(self):
    if self.userConn:
      user_info = self.userConn["user_info"]
      conn_info = self.userConn["conn_info"]
      try:
        accounts = self.da.get_accounts(conn_info["connectionID"])
        accList = []
        for a in accounts:
          accList.append((a["name"], a))
        self.printTupleList(accList)
        choice = int(raw_input("Choose account?: "))
        accountID = accList[choice][1]["accountID"]
        return accountID
      except:
        print("ERROR: Could not choose account")
        raise
    else:
      print("ERROR: Could not get either user or connection")

  def choose_institution(self):
    all_inst = self.da.get_institutions()
    instMap = {}
    for inst in all_inst:
      instMap[inst["name"]] = inst["institutionID"]
    instList = instMap.keys()
    self.printList(instList)
    choice = int(raw_input("Institution?: "))
    instID = instMap[instList[choice]]
    return instID 
  
  def check_user(self):
    username = raw_input("Username?: ")
    password = getpass.getpass()
    is_auth = self.da.check_password(username, password)
    if is_auth:
      return username
    else:
      print("ERROR: No such user/password combination")
      return None

  def printList(self, aList):  
    for i in range(len(aList)):
      print("{}: {}".format(i, aList[i]))
 
  def printTupleList(self, aList):
    for i in range(len(aList)):
      print("{}: {}".format(i, aList[i][0]))
  
  topMenu = [
    ("exit", None),
    ("register new user", add_user),
    ("log in", check_user)
  ]
  
  secondMenu = [
    ("exit", None),
    ("add institution", add_institution),
    ("add connection", add_connection),
    ("get transactions", get_transactions),
    ("get accounts", get_plaid_accounts),
    ("upload from file", upload_from_file)
  ]

  thirdMenu = [
    ("exit", None),
    ("add institution", add_institution),
    ("add connection", add_connection)
  ]
  
def main():
  mu = ManualUpdater("instance/moneygeek.sqlite3")

  exit = False
  da = DA("instance/moneygeek.sqlite3")
  while not exit:
    print("-------------------------")
    mu.printTupleList(mu.topMenu)
    option = int(raw_input("Choose option: "))
    command = mu.topMenu[option][1]
    if command:
      try:
        res = command(mu)
        if res:
          mu.set_user(res)
          exit = True     
      except IOError as e:
        print(e.message)
        exit = True
    else:
      exit = True
    time.sleep(2)
  exit = False
  while not exit:
    print("------------{}-------------".format(mu.username))
    if mu.userConn:
      menu = mu.secondMenu
    else:
      menu = mu.thirdMenu
    mu.printTupleList(menu)
    option = int(raw_input("Choose option: "))
    command = menu[option][1]
    if command:
      try:
        res = command(mu)
      except IOError as e:
        print(e.message)
        exit = True
    else:
      exit = True
    time.sleep(2)
  print("Have a nice day!")

if __name__ == "__main__":
  main()

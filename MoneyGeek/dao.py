import sqlite3
from sqlite3 import IntegrityError
from datetime import datetime
from werkzeug.security import check_password_hash, generate_password_hash

class DataAccessor:

  def __init__(self, db_file):
    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    self.conn = conn
    self.db = conn.cursor()

  def initialise_db(self, sql_file):
    try:
      with open(sql_file, 'r', encoding='utf8') as f:
        self.db.executescript(f.read())
    except IOError:
      print("Could not find sql script file: {}".format(sql_file))

  ##### Adding data (can throw exceptions) ####

  def add_user(self, username, password, email):
    existing = self.get_user_info(username, email)
    if not existing:
      self.db.execute(
        'INSERT INTO user (username, password, email) VALUES (?, ?, ?)',
          (username, generate_password_hash(password), email)
      )
      self.conn.commit()
    else:
      self.conn.rollback()
      raise(IOError("ERROR: Username: [{}] or Email: [{}] already exists!".format(existing['username'], existing['email'])))
  
  def add_account(self, connectionID, accountID, lastFourID, name, officialName, 
      accountType, accountSubType):
    existing = self.get_account_info(connectionID, accountID)
    if not existing:
      self.db.execute(
        "INSERT INTO account (connectionID, accountID, lastFourID, name, officialName, accountType, "
          + "accountSubType) VALUES (?, ?, ?, ?, ?, ?, ?)",
          (connectionID, accountID, lastFourID, name, officialName, accountType, accountSubType)
      )
      self.conn.commit()
    else:
      self.conn.rollback()
      raise(IOError("ERROR: AccountID already exists!"))

  def add_institution(self, institutionID, name):
    try:
      self.db.execute(
        "INSERT INTO institution (institutionID, name) VALUES (?, ?)", (institutionID, name)
      )
      self.conn.commit()
    except IntegrityError:
      self.conn.rollback()
      raise(IOError("ERROR: InstitutionID already exists!"))

  def add_connection(self, userID, institutionID, accessCode):
    existing = self.get_connection_info(userID, institutionID)
    if not existing:
      self.db.execute(
        "INSERT INTO connection (userID, institutionID, accessCode) VALUES (?, ?, ?)", (userID, institutionID, accessCode)
      )
      self.conn.commit()
    else:
      self.db.execute(
        "UPDATE connection SET accessCode = ? WHERE userID = ? AND institutionID = ?", (accessCode, userID, institutionID)
      )
      self.conn.commit()

  def add_transaction(self, transactionID, accountID, category, subCategory, name, amount, date, categoryID):
    try:
      self.db.execute(
        "INSERT INTO txn (transactionID, accountID, category, subCategory, name, amount, date, categoryID) VALUES "
          + "(?, ?, ?, ?, ?, ?, ?, ?)",
          (transactionID, accountID, category, subCategory, name, amount, date, categoryID)
      )
      self.conn.commit()
    except sqlite3.IntegrityError:
      self.conn.rollback()
      raise

  def add_category(self, name):
    try:
      self.db.execute(
        "INSERT INTO category (name) VALUES (?)", (name, )
      )
      self.conn.commit()
    except sqlite3.IntegrityError:
      self.conn.rollback()
      raise

  #### Updating data (can throw exception) ####

  def update_category(self, accountID, transactionID, categoryID):
    try:
      self.db.execute(
        "UPDATE txn SET categoryID = ? WHERE accountID = ? AND transactionID = ?",
        (categoryID, accountID, transactionID)
      )
      self.conn.commit()
    except:
      self.conn.rollback()
      raise

  def upsert_budget(self, userID, categoryID, budgetAmount):
    try:
      res = self.db.execute(
        "SELECT 1 FROM budget where userID = ? and categoryID = ?",
        (userID, categoryID)
      )
      if res.fetchone():
        # Update existing record
        self.db.execute(
          "UPDATE budget SET budgetAmount = ? WHERE userID = ? and categoryID = ?",
          (budgetAmount, userID, categoryID)
        )
      else:
        # Insert new record
        self.db.execute(
          "INSERT INTO budget (userID, categoryID, budgetAmount) VALUES (?,?,?)",
          (userID, categoryID, budgetAmount)
        )
      self.conn.commit()
    except:
      self.conn.rollback()
      raise

  #### Getting data (can return None) ####
  
  def get_category_info(self, category):
    res = self.db.execute(
      "SELECT categoryID, name FROM category WHERE UPPER(name) = UPPER(?)", (category, )
    )
    row = res.fetchone()
    return row

  def get_user_info(self, username, email=""):
    res = self.db.execute(
      "SELECT userID, username, password, email FROM user WHERE username = ? OR email = ?", (username, email)
    )
    row = res.fetchone()
    return row

  def get_user_from_id(self, userID):
    res = self.db.execute(
      "SELECT userID, username, email FROM user WHERE userID = ?", (userID, )
    )
    row = res.fetchone()
    return row

  def get_account_info(self, connectionID, accountID):
    res = self.db.execute(
      "SELECT connectionID, accountID, lastFourID, name, officialName, accountType, accountSubType "
        + "FROM account WHERE connectionID = ? AND accountID = ?", (connectionID, accountID)
    )
    row = res.fetchone()
    return row

  def get_institution_info(self, institutionID):
    res = self.db.execute(
      "SELECT institutionID, name FROM institution WHERE institutionID = ?", (institutionID, )
    )
    row = res.fetchone()
    return row

  def get_user_connections(self, userID):
    res = self.db.execute(
      "SELECT c.connectionID, c.userID, c.institutionID, c.accessCode, i.name FROM connection c, institution i WHERE "
        + "c.institutionID = i.institutionID AND c.userID = ?", (userID,)
    )
    row = res.fetchall()
    return row

  def get_account_connection(self, userID, accountID):
    res = self.db.execute(
      "SELECT c.connectionID, c.institutionID, c.accessCode, a.accountID, a.lastFourID, a.name, a.officialName "
        + "FROM connection c, account a WHERE c.userID = ? AND c.connectionID = a.connectionID "
        + "AND a.accountID = ?", (userID, accountID)
    )
    row = res.fetchone()
    return row

  def get_connection_info(self, userID, institutionID):
    res = self.db.execute(
      "SELECT connectionID, userID, institutionID, accessCode FROM connection WHERE userID = ? AND institutionID = ?", (userID, institutionID)
    )
    row = res.fetchone()
    return row

  def get_transaction_info(self, userID, transactionID, accountID):
    res = self.db.execute(
      "SELECT userID, transactionID, accountID, category, subCategory, name, amount, date, categoryID FROM txn "
        + "WHERE userID = ? AND transactionID = ? AND accountID = ?", (userID, transactionID, accountID) 
    )
    row = res.fetchone()
    return row

  #### Getting bulk data ###
  
  def get_all_userID(self):
    res = self.db.execute(
      "SELECT userID, username FROM user"
    )
    return res.fetchall()

  def get_institutions(self):
    res = self.db.execute(
      "SELECT institutionID, name FROM institution"
    )
    return res.fetchall()

  def get_accounts(self, connectionID):
    res = self.db.execute(
      "SELECT connectionID, accountID, lastFourID, name, officialName, accountType, accountSubType FROM account "
        + "WHERE connectionID = ?", (connectionID, )
    )
    return res.fetchall()
 
  def get_categories(self):
    res = self.db.execute(
      "SELECT categoryID, name FROM category"
    )
    return res.fetchall()

  def get_available_dates(self, accountID):
    res = self.db.execute(
      "SELECT distinct strftime(\"%Y\", t.date) AS year, strftime(\"%m\", t.date) AS month from txn t "
        + "WHERE accountID = ? ORDER BY year desc, month desc", (accountID, )
    )
    return res.fetchall()

  def get_transactions(self, accountID):
    res = self.db.execute(
      "SELECT t.transactionID, t.accountID, t.category, t.subCategory, t.name, t.amount, t.date, t.categoryID, c.name as userCategory "
      + "FROM txn t LEFT JOIN category c ON t.categoryID = c.categoryID WHERE accountID = ?", (accountID, )
    )
    return res.fetchall()

  def get_transactions_for_month(self, accountID, year, month):
    res = self.db.execute(
      "SELECT t.transactionID, t.accountID, t.category, t.subCategory, t.name, t.amount, t.date, t.categoryID, c.name as userCategory "
      + "FROM txn t LEFT JOIN category c ON t.categoryID = c.categoryID WHERE accountID = ? "
      + "AND strftime(\"%Y\", t.date) = ? AND strftime(\"%m\", t.date) = ?", (accountID, year, month)
    )
    return res.fetchall()

  def get_summary(self, userID):
    res = self.db.execute(
      "SELECT c.connectionID, i.name AS institution, a.accountID, a.name as account, a.accountID, "
        + "a.accountType, a.accountSubType, a.lastFourID, a.officialName FROM connection c, account a, institution i "
        + "WHERE c.userID = ? AND c.institutionID = i.institutionID AND c.connectionID = a.connectionID "
        + "ORDER BY institution, a.name",
        (userID, )
    )
    return res.fetchall()

  #### Canned Queries ####
  def find_uncategorised_accounts(self, userID):
    res = self.db.execute(
      "SELECT lastFourID, strftime(\"%Y\", t.date) AS year, strftime(\"%m\", t.date) AS month "
        + "FROM conection c, txn t, account a WHERE c.userID = ? AND c.connectionID = a.connectionID "
        + "AND a.accountID = t.accountID AND t.categoryID IS NULL "
        + "GROUP BY lastFourID, year, month",
        (userID, )
    )
    return res.fetchall()

  def get_monthly_summary(self, userID, year, month):
    res = self.db.execute(
      "SELECT strftime('%Y', t.date) AS year, "
        + "strftime('%m', t.date) AS month, "
        + "COALESCE(cat.name, 'Uncategorised') AS catName, "
        + "ROUND(SUM(t.amount),2) AS total, "
        + "COALESCE(b.budgetAmount, 0) AS budget "
        + "FROM connection c, account a, txn t "
        + "LEFT JOIN category cat ON t.categoryID = cat.categoryID " 
        + "LEFT JOIN budget b ON t.categoryID = b.categoryID "
        + "WHERE c.userID = ? AND c.connectionID = a.connectionID "
        + "AND a.accountID = t.accountID AND year = ? and month = ? GROUP BY catName ORDER BY catName",
        (userID, year, month)
    )
    return res.fetchall()

  def get_annual_summary(self, userID, year):
    res = self.db.execute(
      "SELECT c.name, c.categoryID, x.month, x.year, ROUND(SUM(x.amount),2) as amount "
      + "FROM category c LEFT JOIN "
      + "  (SELECT strftime(\"%Y\", t.date) AS year, strftime(\"%m\", t.date) AS month, t.amount, t.categoryID "
      + "  FROM connection c, account a, txn t "
      + "  WHERE c.userID = ? AND c.connectionID = a.connectionID and a.accountID = t.accountID)x "
      + "ON c.categoryID = x.categoryID WHERE year = ? GROUP BY c.name, c.categoryID, x.month, x.year ORDER BY c.name, x.month, x.year",
      (userID, year)
    )
    return res.fetchall()

  def get_budget(self, userID):
    res = self.db.execute(
      "SELECT categoryID, budgetAmount FROM budget WHERE userID = ?", (userID, )
    )
    return res.fetchall()

  #### Validate ####

  def check_password(self, username, password):
    user_info = self.get_user_info(username, "")
    if not user_info:
      return False
    else:
      return check_password_hash(user_info['password'], password)

  #### Utility functions ####

  def convertDateToSQLDate(self, strDate, fmt="%m/%d/%y"):
    date = datetime.strptime(strDate, fmt)
    return date.isoformat()

  def convertRowsToDictList(self, rows):
    results = []
    if not rows or len(rows) == 0:
      return results
    keys = rows[0].keys()
    for r in rows:
      m = {}
      for key in keys:
        m[key] = r[key]
      results.append(m)
    return results


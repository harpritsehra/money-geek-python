import os
import sqlite3
import unittest
from MoneyGeek.dao import DataAccessor

class TestDataAccessor(unittest.TestCase):

  def setUp(self):
    self.da = DataAccessor('instance/test.sqlite3')
    self.da.initialise_db('db/schema.sql')
    self.da.initialise_db('db/categories.sql')

  def test_add_user(self):
    user = "testUser"
    password = "testPass"
    invalid_user = "someOtherUser"
    invalid_email = "someOtherEmail"
    email = "test@email.com"
    # Add valid user
    self.da.add_user(user, password, email)
    user_info = self.da.get_user_info(user, email)
    self.assertEqual(1, user_info["userID"])
    # Get user that does not exist
    user_info = self.da.get_user_info(invalid_user, invalid_email)
    self.assertEqual(None, user_info)
    # Try adding existing user again with new email
    try:
      self.da.add_user(user, password, email + "_new")
      self.assertTrue(False)
    except IOError:
      self.assertTrue(True)
    # Try adding existing email again with new user
    try:
      self.da.add_user(user + "_new", password, email)
      self.assertTrue(False)
    except IOError:
      self.assertTrue(True)

  def test_add_account(self):
    connectionID = "1"
    institutionID = "institution-2"
    accountID = "account-1"
    lastFourID = 1234
    name = "Test_Bank"
    officialName = "Real_Bank"
    accountType = "Checking"
    accountSubType = "Sub_acct"
    # Add valid account
    self.da.add_account(connectionID, accountID, lastFourID, name, officialName, accountType, accountSubType)
    acct_info = self.da.get_account_info(connectionID, accountID)
    self.assertEqual(name, acct_info["name"])

  def test_add_institution(self):
    institutionID = "i1"
    name = "test bank"
    # Add valid institution
    self.da.add_institution(institutionID, name)
    inst_info = self.da.get_institution_info(institutionID)
    self.assertEqual(name, inst_info["name"])
    # Add a duplicate
    try:
      self.da.add_institution(institutionID, name)
      self.assertTrue(False)
    except IOError:
      self.assertTrue(True)
    # Add another and get bulk
    self.da.add_institution(institutionID + "_another", name)
    all_inst = self.da.get_institutions()
    self.assertEqual(2, len(all_inst))

  def test_add_connection(self):
    userID = 1
    institutionID = "i1"
    accessCode = "accessTEST"
    # Add valid connection
    self.da.add_connection(userID, institutionID, accessCode)
    conn_info = self.da.get_connection_info(userID, institutionID)
    self.assertEqual(accessCode, conn_info["accessCode"])

  def test_add_transaction(self):
    txnId = "txn1"
    accountID = "account1"
    category = "test"
    subCategory = "subtest"
    name = "purchase 1"
    amount = 100
    date = "2018-01-13"
    categoryID = None
    self.da.add_transaction(txnId, accountID, category, subCategory, name, amount, date, categoryID)
    # Try adding a duplicate
    try:
      self.da.add_transaction(txnId, accountID, category, subCategory, name, amount, date, categoryID)
      self.assertTrue(False)
    except sqlite3.IntegrityError:
      self.assertTrue(True)

  def test_get_category_info(self):
    row = self.da.get_category_info("Groceries")
    self.assertEqual(1, row["categoryID"])
    row = self.da.get_category_info("invalid_category")
    self.assertEqual(None, row)

  def test_get_summary(self):
    # Setup!
    user = "testUser"
    inst1 = "testInst1"
    inst2 = "testInst2"
    self.da.add_user(user, "testPass", "testEmail")
    user_info = self.da.get_user_info(user)
    userID = user_info["userID"]
    self.da.add_institution(inst1, "institution 1")
    self.da.add_institution(inst2, "institution 2")
    self.da.add_connection(userID, inst1, "abcd")
    self.da.add_connection(userID, inst2, "efgh")
    conn1_info = self.da.get_connection_info(userID, inst1)
    conn2_info = self.da.get_connection_info(userID, inst2)
    self.da.add_account(conn1_info["connectionID"], "testAcc1", 1234, "account 1", "account 1 official", "Test1", "TestSub1")
    self.da.add_account(conn1_info["connectionID"], "testAcc2", 5678, "account 2", "account 2 official", "Test2", "TestSub2")
    self.da.add_account(conn1_info["connectionID"], "testAcc3", 9101, "account 3", "account 3 official", "Test3", "TestSub3")
    # Actual test!
    summary = self.da.get_summary(userID)
    self.assertEqual(3, len(summary))

  def test_get_monthly_summary(self):
    # Setup!
    user = "testUser"
    inst1 = "testInst1"
    accountID = "testAcc1"
    cat1 = "Category-1"
    cat2 = "Category-2"
    self.da.add_user(user, "testPass", "testEmail")
    user_info = self.da.get_user_info(user)
    userID = user_info["userID"]
    self.da.add_institution(inst1, "institution 1")
    self.da.add_connection(userID, inst1, "abcd")
    conn1_info = self.da.get_connection_info(userID, inst1)
    self.da.add_account(conn1_info["connectionID"], accountID, 1234, "account 1", "account 1 official", "Test1", "TestSub1")
    self.da.add_category(cat1)
    cat1ID = self.da.get_category_info(cat1)["categoryID"]
    self.da.add_category(cat2)
    cat2ID = self.da.get_category_info(cat2)["categoryID"]
    self.da.add_transaction("id1", accountID, "no cat", "no subcat", "txn-1", 100, "2018-01-15", None)
    self.da.add_transaction("id2", accountID, "no cat", "no subcat", "txn-2", 150, "2018-01-16", cat1ID)
    self.da.add_transaction("id3", accountID, "no cat", "no subcat", "txn-3", -100, "2018-01-17", cat1ID)
    self.da.add_transaction("id4", accountID, "no cat", "no subcat", "txn-4", 3, "2018-01-18", cat2ID)
    self.da.add_transaction("id5", accountID, "no cat", "no subcat", "txn-5", 1000, "2018-01-18", cat2ID)
    self.da.add_transaction("id6", accountID, "no cat", "no subcat", "txn-6", -10, "2018-02-15", cat2ID)
    # Actual test!
    summary = self.da.get_monthly_summary(userID, '2018', '01')
    self.assertEqual(3, len(summary))
    summary = self.da.get_monthly_summary(userID, '2018', '02')
    self.assertEqual(1, len(summary))

  def tearDown(self):
    os.remove('instance/test.sqlite3')
    #pass

if __name__ == '__main__':
  unittest.main()

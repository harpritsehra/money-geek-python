import sys
import json
import re
from datetime import datetime
from collections import namedtuple

def upload_data(db, userID, type_enum, billingMonth, billingYear, data):
    error = None
    parsed_data = []
    try:
        typeID = get_typeID(db, type_enum)
        lines = data.split("\n")
        if type_enum == "HSBC_CC":
            parsed_data = parse_HSBC_CC_data(lines)
        elif type_enum == "HSBC_CHK":
            parsed_data = parse_HSBC_CHK_data(lines)
        elif type_enum == "Virgin_CC":
            parsed_data = parse_Virgin_CC_data(lines)
        else:
            raise UnsupportedOptionError("Type:{} is not currently supported!".format(type_enum))

        db.execute("BEGIN TRANSACTION")
        print("BEGUN!!!")
        statementID = insert_statement(db, userID, typeID, billingMonth, billingYear)
        print("INSERTED_STATEMENT")
        for entry in parsed_data:
            categoryID = None
            if "category" in entry:
                categoryID = get_categoryID(db, entry["category"])
            else:
                categoryID = try_and_map(db, entry["desc"])
            insert_txn(db, statementID, userID, categoryID, entry["date"], entry["desc"], entry["amount"])
            print("INSERTED!!!")
    except DataParseError as e:
        error = e.message
        print("EXCEPTION!!!")
    except DatabaseError as e:
        error = e.message
        print("EXCEPTION!!!")
    except UnsupportedOptionError as e:
        error = e.message
        print("EXCEPTION!!!")

    if error:
        db.rollback()
    else:
        db.commit()
    return error

def insert_txn(db, statementID, userID, categoryID, date, desc, amount):
    db.execute(
        "INSERT INTO txn(statementID, userID, categoryID, transDate, description, amount)" +
        "values(?, ?, ?, ?, ?, ?)",
        (statementID, userID, categoryID, date, desc, amount)
    )

def insert_statement(db, userID, typeID, billingMonth, billingYear):
    if get_statementID(db, userID, typeID, billingMonth, billingYear):
        raise DatabaseError("Statement for userID:{} typeID:{}, billingMonth:{}, billingYear{} already exists!".format(
            userID, typeID, billingMonth, billingYear))

    db.execute("INSERT INTO statement(userID, typeID, billingMonth, billingYear) " +
        "values(?, ?, ?, ?)", 
            (userID, typeID, billingMonth, billingYear)
    )
    return get_statementID(db, userID, typeID, billingMonth, billingYear)

def get_statementID(db, userID, typeID, billingMonth, billingYear):
    res = db.execute("SELECT statementID FROM statement WHERE userID = ? AND typeID = ? AND billingMonth = ? AND billingYear = ?", (userID, typeID, billingMonth, billingYear))
    row = res.fetchone()
    if row:
        return row['statementID']
    else:
        return None

def get_all_statements(db, userID):
    res = db.execute("SELECT t.description, s.billingMonth, s.billingYear, s.statementID, sum(case when txn.amount < 0 then txn.amount else 0 end) debits, sum(case when txn.amount > 0 then txn.amount else 0 end) credits FROM statement s, type t, txn WHERE s.typeID = t.typeID AND s.userID = ? and s.statementID = txn.statementID GROUP BY t.description, s.billingMonth, s.billingYear, s.statementID ORDER BY s.billingYear, s.billingMonth, s.typeID", (userID, ))

    stmt = []
    row = res.fetchone()
    while row:
        stmt.append((row['description'], row['billingMonth'], row['billingYear'], row['statementID'], row['debits'], row['credits']))
        row = res.fetchone()
    return stmt

def get_all_txns(db, statementID):
    res = db.execute("SELECT t.categoryID, DATE(t.transDate) as transDate, t.description, t.amount, c.name, t.txnID FROM txn t LEFT OUTER JOIN category c ON t.categoryID = c.categoryID WHERE t.statementID = ?", (statementID, ))
    txns = []
    row = res.fetchone()
    while row:
        txns.append((row['categoryID'], row['transDate'], row['description'], row['amount'], row['name'], row['txnID']))
        row = res.fetchone()
    return txns

def get_all_categories(db):
    res = db.execute("SELECT categoryID, name FROM category")
    cat = []
    row = res.fetchone()
    while row:
        cat.append((row['categoryID'], row['name']))
        row = res.fetchone()
    return cat

def get_categoryID(db, cat_name):
    res = db.execute("SELECT categoryID from category where UPPER(name) = UPPER(?)", (cat_name, ))
    row = res.fetchone()
    if row:
        return row['categoryID']
    else:
        raise DatabaseError("Could not find categoryID for name: {}".format(cat_name))

def get_typeID(db, type_enum):
    res = db.execute("SELECT typeID from type where enum = ?", (type_enum, ))
    row = res.fetchone()
    if row:
        return row['typeID']
    else:
        raise DatabaseError("Could not find typeID for enum: {}".format(type_enum))

def update_category(db, txnID, newCat):
    m = re.search("\((.*)\)", newCat)
    error = None
    if m:
        categoryID = int(m.groups()[0])
        db.execute("UPDATE txn SET categoryID = ? where txnID = ?", (categoryID, txnID))
        db.commit()
    else:
        error = "Some error occurred when trying to update txn with txnID = {} and new category = {}".format(txnID, newCat)
    return error

def parse_HSBC_CC_data(lines):
    for line in lines:
        line = line.strip()
        print("Line = " + line)
        elems = line.split("\t")
        if len(elems) == 4 or len(elems) == 5:
            res = {}
            res["date"] = convertDateToSQLDate(elems[0])
            res["desc"] = elems[2]
            res["amount"] = convertAmountToFloat(elems[3], flip=True)
            if len(elems) == 5:
                res["category"] = elems[4]
            yield(res)
        else:
            errorMsg = "Invalid format: HSBC_CC data should be 4 (or 5) tab delimited columns: date, date, desc, amount, (category). Found: {}".format(str(len(elems)))
            print(errorMsg)
            raise DataParseError(errorMsg)

def parse_HSBC_CHK_data(lines):
    for line in lines:
        line = line.strip()
        print("Line = " + line)
        elems = line.split("\t")
        if len(elems) == 3:
            res = {}
            res["date"] = convertDateToSQLDate(elems[0], fmt="%m/%d/%Y")
            res["desc"] = elems[1]
            res["amount"] = convertAmountToFloat(elems[2])
            yield(res)
        else:
            errorMsg = "Invalid format: HSBC_CHK data should be 3 tab delimited columns: date, desc, amount. Found: {}".format(str(len(elems)))
            print(errorMsg)
            raise DataParseError(errorMsg)

def parse_Virgin_CC_data(lines):
    for line in lines:
        line = line.strip()
        print("Line = " + line)
        elems = line.split("\t")
        if len(elems) == 5:
            res = {}
            res["date"] = convertDateToSQLDate(elems[0], fmt="%m/%d/%Y")
            res["desc"] = elems[2]
            res["amount"] = convertAmountToFloat(elems[4])
            yield(res)
        else:
            errorMsg = "Invalid format: Virgin_CC data should have 5 tab delimited columns: date, ref, desc, address, amount. Found: {}".format(str(len(elems)))
            print(errorMsg)
            raise DataParseError(errorMsg)

def try_and_map(db, desc):
    res = db.execute("select categoryID from mapping where ? like pattern", (desc, ))
    row = res.fetchone()
    if row:
        return row["categoryID"]
    else:
        return None

def convertAmountToFloat(amount, flip=False):
    if amount.startswith("$"):
        amount = amount[1:]
    amount = amount.replace(",", "")
    if flip:
        return float(amount) * -1
    else:
        return float(amount)

def convertDateToSQLDate(strDate, fmt="%m/%d/%y"):
    date = datetime.strptime(strDate, fmt)
    return date.isoformat()

class DataParseError(Exception):
    def __init__(self, message):
        self.message = message

class DatabaseError(Exception):
    def __init__(self, message):
        self.message = message

class UnsupportedOptionError(Exception):
    def __init__(self, message):
        self.message = message

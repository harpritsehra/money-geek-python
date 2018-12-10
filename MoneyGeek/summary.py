import functools
import json
import datetime
import calendar

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from .flask_util import get_da

bp = Blueprint('summary', __name__, url_prefix='/summary')

@bp.route('/home', methods=('GET',))
def home():
  userID = session.get('userID')
  da = get_da()
  error = None
  s = da.convertRowsToDictList(da.get_summary(userID))
  session['summary'] = s
  if not error:
    return render_template('summary/home.html')
  flash(error)
  return redirect(url_for('auth.login'))

@bp.route('/view_account', methods=('POST', ))
def view_account():
  # expects POST with:
  # accountID passed from summary page (or reposted from view_account page)
  # userID should be available in session
  error = None
  userID = session.get('userID')
  info = {}
  
  # Check accountID presence
  accountID = request.form.get('accountID')
  if not accountID:
    error = "AccountID is missing"
  else:
    # Store accountID
    info['accountID'] = accountID
    da = get_da()
    # Get connection, account, txn info
    account_conn = da.get_account_connection(userID, accountID)
    accountName = account_conn['name']
    # Store accountName
    info['name'] = accountName
    dates = da.convertRowsToDictList(da.get_available_dates(accountID))
    # Store dates
    info['dates'] = dates
    year = None
    month = None
    transactions = None
    categories = None
    # choose and store date
    chosen_date = request.form.get('chosen_date')
    if chosen_date:
      (year, month) = chosen_date.split('-')
    elif len(dates) > 0:
      year = dates[0]['year']
      month = dates[0]['month']
    if year and month:
      info['year'] = year
      info['month'] = month
      categories = da.convertRowsToDictList(da.get_categories())
      transactions = da.convertRowsToDictList(da.get_transactions_for_month(accountID, year, month))

  if not error:
    return render_template('summary/view_account.html', transactions=transactions, categories=categories, info=info)
  else:
    flash(error)
    return redirect(url_for('summary.home'))

@bp.route('/update_transactions', methods=('POST', ))
def update_transactions():
  accountID = request.form.get('accountID')
  txns = request.form.getlist('txn')
  da = get_da()
  errCount = 0
  for txn in txns:
    elems = txn.split(':')
    if len(elems) == 3:
      da.update_category(accountID, elems[0], elems[2])
    elif len(elems) == 2 and elems[1] == '':
      da.update_category(accountID, elems[0], None)
    else:
      errCount += 1
  if errCount > 0:
    flash("{} updates were invalid and skipped".format(errCount))
  return redirect(url_for('summary.view_account'), code=307)

@bp.route('/monthly_summary', methods=('GET', ))
def monthly_summary():
  error = None
  userID = session.get('userID')
  year = request.args.get('year')
  month = request.args.get('month')
  if not year or not month:
    now = datetime.datetime.now()
    year = str(now.year)
    month = str(now.month)
  da = get_da()
  summary = da.convertRowsToDictList(da.get_monthly_summary(userID, year, month))
  return render_template('summary/monthly_summary.html', year=year, month=month, summary=summary)

@bp.route('/annual_summary', methods=('GET', ))
def annual_summary():
  error = None
  userID = session.get('userID')
  year = request.args.get('year')
  cumulative = request.args.get('cumulative')
  if not year:
    now = datetime.datetime.now()
    year = str(now.year)
  if cumulative == 'true':
    cumulative = True
  else:
    cumulative = False
  da = get_da()
  res = da.get_annual_summary(userID, year)
  budget = da.get_budget(userID)
  budgetMap = {}     # Category ID -> budget amount
  for row in budget:
    budgetMap[row['categoryID']] = row['budgetAmount']
  categoryIDMap = {} # Category name -> Category ID
  cat_store = None   # Category name @ [0], amount at month index (1 = Jan), total annual for category @ [13], monthly budget @ [14]
  summary = []       # An array of cat_stores
  total = 0
  for row in res:
    if not cat_store or cat_store[0] != row['name']:
      # New Category - so create a new row
      cat_store = [0 for _ in range(15)]
      cat_store[0] = row['name']
      summary.append(cat_store)
      categoryIDMap[row['name']] = row['categoryID']
      total = 0
    cat_store[int(row['month'])] = row['amount']
    total += row['amount']
    cat_store[13] = total
    cat_store[14] = budgetMap.get(row['categoryID'], 0)

  if cumulative:
    for cat_store in summary:
      cum = 0;
      for i in range(1, 13):
        cum += cat_store[i]
        cat_store[i] = cum
  
  months = [calendar.month_abbr[i+1] for i in range(12)]
  return render_template('summary/annual_summary.html', year=year, summary=summary, categoryIDMap=categoryIDMap, months=months, cumulative=cumulative)

@bp.route('/update_budget', methods=('POST', ))
def update_budget():
  # User clicks update and a form is posted with the categoryID as the key and the budget amount as the value
  error = None
  userID = session.get('userID')
  da = get_da()
  for categoryID in request.form.keys():
    da.upsert_budget(userID, categoryID, request.form.get(categoryID))
  return redirect(url_for('summary.annual_summary'))

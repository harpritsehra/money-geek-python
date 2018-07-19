import functools

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from billProcessor.db import get_db

import billProcessor.DataFns as fns

bp = Blueprint('summary', __name__, url_prefix='/summary')

@bp.route('/home', methods=('GET',))
def home():
    user_id = session.get('user_id')
    db = get_db()
    error = None
    s = fns.get_all_statements(db, user_id)
    session['statements'] = s
    if not error:
        return render_template('summary/home.html')
    flash(error)

@bp.route('/upload', methods=('GET', ))
def upload():
    db = get_db()
    error = None
    types = None

    res = db.execute('SELECT * from type')
    types = res.fetchall()
    return render_template('summary/upload.html', types=types)

@bp.route('/process_upload', methods=('POST', ))
def process_upload():
    type_enum = request.form['type']
    billingMonth = request.form['month']
    billingYear = request.form['year']
    data = request.form['data']
    error = None

    if not type_enum or not data:
        error = 'Must choose a type and paste some data'
    else:
        db = get_db()
        user_id = session.get('user_id')
        error = fns.upload_data(db, user_id, type_enum, billingMonth, billingYear, data)
    
    if error:
        flash(error)

    return redirect(url_for('summary.home'))


@bp.route('/view_statement', methods=('POST', ))
def view_statement():
    db = get_db()
    statementID = request.form['statementID']
    error = None
    t = fns.get_all_txns(db, statementID)
    session['txns'] = t
    c = fns.get_all_categories(db)
    session['categories'] = c
    if not error:
        return render_template('summary/view_statement.html')
    flash(error)

@bp.route('/update_statement', methods=('POST', ))
def update_statement():
    db = get_db()
    txns = request.form.keys()
    updated = 0
    errors = []
    for txnID in txns:
        if "original" in txnID:
            originalID = txnID.split("-")[1]
            originalCat = request.form[txnID]
            newCat = request.form[originalID]
            if newCat != originalCat:
                error = fns.update_category(db, originalID, newCat)
                if error:
                    errors.append(error)
                updated += 1
    if len(errors) > 0:
        flash("Something went wrong!")
        db.rollback()
        for e in errors:
            flash(e)
    elif updated > 0:
        db.commit()
        flash("Updated {} categories in statement".format(updated))
    
    return redirect(url_for('summary.home'))

import functools

# This is part of the flask app which takes care of authentication

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
from werkzeug.security import check_password_hash, generate_password_hash

from .flask_util import get_da

# All routes are under /auth
bp = Blueprint('auth', __name__, url_prefix='/auth')

# /auth/register
@bp.route('/register', methods=('GET', 'POST'))
def register():
  flash("Registration not implemented!")
  return redirect(url_for('auth.login'))

@bp.route('/login', methods=('GET', 'POST'))
def login():
  if request.method == 'POST':
    username = request.form['username']
    password = request.form['password']
    da = get_da()
    error = None
    user_info = da.get_user_info(username)

    if not user_info:
      error = 'Incorrect username.'
    elif not check_password_hash(user_info['password'], password):
      error = 'Incorrect password.'

    if error is None:
      session.clear()
      session['userID'] = user_info['userID']
      g.user = user_info['username']
      return redirect(url_for('summary.home'))

    flash(error)

  return render_template('auth/login.html')

@bp.before_app_request
def load_logged_in_user():
  user_id = session.get('userID')
  da = get_da()

  if user_id is None:
    g.user = None
  else:
    g.user = da.get_user_from_id(user_id)

@bp.route('/logout')
def logout():
  session.clear()
  return redirect(url_for('summary.home'))

def login_required(view):
  @functools.wraps(view)
  def wrapped_view(**kwargs):
    if g.user is None:
      return redirect(url_for('auth.login'))

    return view(**kwargs)

  return wrapped_view

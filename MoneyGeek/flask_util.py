from flask import current_app, g
from . import dao

def get_da():
  if 'da' not in g:
    g.da = dao.DataAccessor(current_app.config['DATABASE'])
  return g.da

def destroy_da():
  if 'da' in g:
    del g['da']

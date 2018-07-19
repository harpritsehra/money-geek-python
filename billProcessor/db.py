import sqlite3

import click
from flask import current_app, g
from flask.cli import with_appcontext


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            current_app.config['DATABASE'],
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        g.db.row_factory = sqlite3.Row

    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()

def init_db():
    db = get_db()
    load_file(db, 'schema.sql')
    load_file(db, 'static_data/categories.sql')
    load_file(db, 'static_data/types.sql')
    load_file(db, 'static_data/mappings.sql')

def load_file(db, fname):
    try:
        with current_app.open_resource(fname) as f:
            db.executescript(f.read().decode('utf8'))
    except IOError:
        print("Could not find sql script file: {}".format(fname))

@click.command('init-db')
@with_appcontext
def init_db_command():
    """Clear the existing data and create new tables."""
    init_db()
    click.echo('Initialized the database.')

def init_app(app):
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)

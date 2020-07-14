import sqlite3

import pyarrow as pa
from cjwmodule import i18n


def validate_database(db: sqlite3.Connection) -> None:
    """Raise sqlite3.DatabaseError if `db` does not point to a database."""
    db.execute("SELECT 1 FROM projects LIMIT 1")


def _cursor_to_table(cursor: sqlite3.Cursor) -> pa.Table:
    colnames = (t[0] for t in cursor.description)
    rows = cursor.fetchall()
    return pa.table(
        {
            colname: pa.array(r[colindex] for r in rows)
            for colindex, colname in enumerate(colnames)
        }
    )


def query_database(db: sqlite3.Connection) -> pa.Table:
    """Return a table; raise sqlite3.ProgrammingError if queries fail."""
    with db.cursor() as c:
        c.execute(
            """
            SELECT 1
            FROM projects
            """
        )
        return _cursor_to_table(c)


def render(arrow_table, params):
    if params["file"] is None:
        return []

    with sqlite3.connect(params["file"]) as db:
        try:
            validate_database(db)
        except sqlite3.DatabaseError:
            return i18n.trans(
                "error.invalidFile", "Please upload a valid SQLite3 file."
            )

        try:
            return query_database(db)
        except sqlite3.ProgrammingError:
            return i18n.trans("error.queryError", "Please upload a newer file.")

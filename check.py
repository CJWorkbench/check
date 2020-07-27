import contextlib
import dateutil.parser
import re
import tempfile
from typing import ContextManager, List, NamedTuple, Optional, Tuple, Union

import lz4.frame
import numpy as np
import pyarrow as pa
import pyarrow.ipc
import sqlite3
from cjwmodule import i18n

# Sqlite3 doesn't have a "recordset" concept, and it won't expose the decltypes
# of a recordset. It will only expose the column names. That isn't good enough:
# what about empty resultsets, or all-null columns?
#
# Solution: infer the column type from its name.
#
# Here are the TYPES we output:
#
# * id: integer without thousands separator. FIXME nix thousands separator!
# * integer: integer
# * timestamp: moment in time
# * text: text field
# * TODO float?
#
# Here are the RULES to get to name from column:
#
# * Column name ending in ' [text]' etc: override type (and nix the " [text]"
#   from the column name).
# * Column name ending in '_id': id
# * Column name ending in '_at': timestamp

REQUESTS_AND_CLAIMS_SQL = r'''
WITH
smooch_requests AS (
  SELECT
    json_extract(daf.value_json, '$.source.originalMessageId') AS whatsapp_message_id,
    json_extract(daf.value_json, '$.authorId') AS whatsapp_user_id,
    json_extract(daf.value_json, '$.text') AS whatsapp_text,
    a.created_at,
    a.id AS annotation_id,
    a.annotated_id AS project_media_id
  FROM dynamic_annotation_fields daf
  INNER JOIN annotations a ON a.id = daf.annotation_id
  WHERE daf.field_name = 'smooch_data'
),
statuses AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY a.annotated_id ORDER BY a.id DESC) AS rn_desc,
    a.annotated_id AS project_media_id,
    u.login AS login,
    daf.value -- JSON-encoded String value
  FROM dynamic_annotation_fields daf
  INNER JOIN annotations a ON a.id = daf.annotation_id
  LEFT JOIN users u ON a.annotator_id = u.id
  WHERE a.annotation_type = 'verification_status'
    AND a.annotated_type = 'ProjectMedia'
    AND daf.field_name = 'verification_status_status'
),
last_statuses AS (
  SELECT
    project_media_id,
    login,
    json_extract(value, '$') AS status -- it's a JSON-encoded String
  FROM statuses
  WHERE rn_desc = 1
),
project_media_list1s AS (
  SELECT
    project_media_projects.project_media_id,
    MIN(projects.title) AS list1
  FROM project_media_projects
  INNER JOIN projects ON project_media_projects.project_id = projects.id
  GROUP BY project_media_projects.project_media_id
),
parent_relationships AS (
  SELECT
    relationships.target_id AS child_project_media_id,
    relationships.source_id AS parent_project_media_id,
    relationships.created_at,
    users.login
  FROM relationships
  LEFT JOIN users ON relationships.user_id = users.id
  WHERE relationships.relationship_type = '---' || CHAR(0xa) || ':source: parent' || CHAR(0xa) || ':target: child' || CHAR(0xa)
),
archived_events AS (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY associated_id ORDER BY id ASC) AS rn,
    associated_id AS project_media_id,
    created_at,
    whodunnit AS user_id
  FROM versions
  WHERE event_type = 'update_projectmedia'
    AND associated_type = 'ProjectMedia'
    AND json_extract(object_changes, '$.archived') = '[false,true]'
),
first_archived_events AS (
  SELECT
    archived_events.project_media_id,
    archived_events.created_at,
    users.login
  FROM archived_events
  LEFT JOIN users ON archived_events.user_id = users.id
  WHERE archived_events.rn = 1
),
publish_related_events AS ( -- events that publish a report, edit a published report, or unpublish a report
  SELECT
    ROW_NUMBER() OVER(PARTITION BY associated_id ORDER BY id ASC) AS rn,
    associated_id AS project_media_id,
    created_at,
    whodunnit AS user_id
  FROM versions
  WHERE event_type IN ('create_dynamic', 'update_dynamic')
    AND associated_type = 'ProjectMedia'
    AND object_changes LIKE '%\nstate: published\n%' -- JSON-encoded YAML value
),
first_publish_events AS (
  SELECT
    publish_related_events.project_media_id,
    publish_related_events.created_at,
    users.login
  FROM publish_related_events
  LEFT JOIN users ON publish_related_events.user_id = users.id
  WHERE publish_related_events.rn = 1
),
status_change_events AS (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY associated_id ORDER BY id ASC) AS rn,
    ROW_NUMBER() OVER(PARTITION BY associated_id ORDER BY id DESC) AS rn_desc,
    associated_id AS project_media_id,
    created_at,
    whodunnit AS user_id
  FROM versions
  WHERE event_type = 'update_dynamicannotationfield'
    AND item_type = 'DynamicAnnotation::Field'
    AND EXISTS (
        SELECT 1
        FROM dynamic_annotation_fields daf
        WHERE daf.id = versions.item_id
          AND daf.annotation_type = 'verification_status'
          AND daf.field_name = 'verification_status_status'
    )
),
first_status_change_events AS (
  SELECT
    status_change_events.project_media_id,
    status_change_events.created_at,
    users.login
  FROM status_change_events
  LEFT JOIN users ON status_change_events.user_id = users.id
  WHERE status_change_events.rn = 1
),
last_status_change_events AS (
  SELECT
    status_change_events.project_media_id,
    status_change_events.created_at,
    users.login
  FROM status_change_events
  LEFT JOIN users ON status_change_events.user_id = users.id
  WHERE status_change_events.rn_desc = 1
),
media_metadatas AS (
  SELECT
    annotations.annotated_id AS media_id,
    dynamic_annotation_fields.value_json AS metadata_json
  FROM dynamic_annotation_fields
  INNER JOIN annotations ON dynamic_annotation_fields.annotation_id = annotations.id
  WHERE annotations.annotated_type = 'Media'
    AND annotations.annotation_type = 'metadata'
    AND dynamic_annotation_fields.field_name = 'metadata_value'
),
project_media_metadatas AS (
  SELECT
    annotations.annotated_id AS project_media_id,
    dynamic_annotation_fields.value_json AS metadata_json
  FROM dynamic_annotation_fields
  INNER JOIN annotations ON dynamic_annotation_fields.annotation_id = annotations.id
  WHERE annotations.annotated_type = 'ProjectMedia'
    AND annotations.annotation_type = 'metadata'
    AND dynamic_annotation_fields.field_name = 'metadata_value'
),
reports AS (
  SELECT
    ROW_NUMBER() OVER(PARTITION BY annotated_id ORDER BY id DESC) AS rn_desc,
    annotated_id AS project_media_id,
    CASE WHEN annotations.data LIKE ('%' || CHAR(0xa) || 'state: published' || CHAR(0xa) || '%') THEN 'published' else 'paused' END AS status
  FROM annotations
  WHERE annotated_type = 'ProjectMedia'
    AND annotation_type = 'report_design'
),
last_reports AS (
  SELECT
    project_media_id,
    status
  FROM reports
  WHERE rn_desc = 1
)
SELECT
  smooch_requests.whatsapp_message_id AS "whatsapp_message_id [text]",
  smooch_requests.whatsapp_user_id AS "whatsapp_user_id [text]",
  smooch_requests.created_at AS requested_at,
  smooch_requests.whatsapp_text,
  project_medias.id AS claim_id,
  last_statuses.status AS claim_status,
  CASE last_statuses.login WHEN 'smooch' THEN NULL ELSE last_statuses.login END AS claim_status_by,
  project_media_list1s.list1 AS claim_list1,
  COALESCE(
    json_extract(project_media_metadatas.metadata_json, '$.title'),
    json_extract(media_metadatas.metadata_json, '$.title')
  ) AS claim_title,
  CASE medias.type WHEN 'Claim' THEN 'Text' ELSE medias.type END AS claim_type,
  medias.url AS claim_url,
  'https://checkmedia.org/' || teams.slug || '/media/' || project_medias.id AS check_url,
  parent_relationships.parent_project_media_id AS primary_claim_id,
  parent_relationships.created_at AS primary_claim_linked_at,
  parent_relationships.login AS primary_claim_linked_by,
  first_status_change_events.created_at AS first_claim_status_changed_at,
  first_status_change_events.login AS first_claim_status_changed_by,
  last_status_change_events.created_at AS last_claim_status_changed_at,
  last_status_change_events.login AS last_claim_status_changed_by,
  last_reports.status AS claim_report_status,
  first_publish_events.created_at AS claim_report_first_published_at,
  first_publish_events.login AS claim_report_first_published_by,
  project_medias.archived AS "claim_archived [integer]",
  first_archived_events.created_at AS claim_first_archived_at,
  first_archived_events.login AS claim_first_archived_by
FROM smooch_requests
INNER JOIN project_medias ON project_medias.id = smooch_requests.project_media_id
INNER JOIN medias ON project_medias.media_id = medias.id
INNER JOIN teams ON teams.id = project_medias.team_id
LEFT JOIN project_media_list1s ON project_media_list1s.project_media_id = project_medias.id
LEFT JOIN last_statuses ON last_statuses.project_media_id = project_medias.id
LEFT JOIN parent_relationships ON parent_relationships.child_project_media_id = project_medias.id
LEFT JOIN first_status_change_events ON first_status_change_events.project_media_id = project_medias.id
LEFT JOIN last_status_change_events ON last_status_change_events.project_media_id = project_medias.id
LEFT JOIN project_media_metadatas ON project_media_metadatas.project_media_id = project_medias.id
LEFT JOIN media_metadatas ON media_metadatas.media_id = medias.id
LEFT JOIN last_reports ON last_reports.project_media_id = project_medias.id
LEFT JOIN first_publish_events ON first_publish_events.project_media_id = project_medias.id
LEFT JOIN first_archived_events ON first_archived_events.project_media_id = project_medias.id
ORDER BY smooch_requests.created_at DESC, project_medias.id
'''


def validate_database(db: sqlite3.Connection) -> None:
    """Raise sqlite3.DatabaseError if `db` does not point to a database."""
    db.execute("SELECT 1 FROM projects LIMIT 1")


class IntegerType:
    def list_to_pyarrow(self, values: List[Optional[int]]) -> pa.Array:
        return pa.array(values, pa.int32())  # TODO dynamic width?


class IdType(IntegerType): pass


class TextType:
    def list_to_pyarrow(self, values: List[Optional[str]]) -> pa.Array:
        return pa.array(values, pa.utf8())


class TimestampType:
    def list_to_pyarrow(self, values: List[Optional[str]]) -> pa.Array:
        return pa.array(
            [(dateutil.parser.isoparse(v) if v else None) for v in values],
            pa.timestamp('ns')
        )


QueryColumnType = Union[IdType, IntegerType, TextType, TimestampType]


class QueryColumn(NamedTuple):
    name: str
    query_column_type: QueryColumnType


def _column_name_to_query_column(name: str) -> QueryColumn:
    match = re.match(r'(.+) \[(id|integer|text|timestamp)\]', name)
    if match:
        name = match.group(1)
        if match.group(2) == 'id':
            type = IdType()
        elif match.group(2) == 'text':
            type = TextType()
        elif match.group(2) == 'integer':
            type = IntegerType()
        elif match.group(2) == 'timestamp':
            type = TimestampType()
        return QueryColumn(name, type)
    elif name.endswith('_id'):
        return QueryColumn(name, IdType())
    elif name.endswith('_at'):
        return QueryColumn(name, TimestampType())
    else:
        return QueryColumn(name, TextType())


def _rows_to_column(rows: list, column_name: str, column_index: int) -> Tuple[str, pa.Array]:
    query_column = _column_name_to_query_column(column_name)
    values = list(r[column_index] for r in rows)
    return query_column.name, query_column.query_column_type.list_to_pyarrow(values)


def _cursor_to_table(cursor: sqlite3.Cursor) -> pa.Table:
    colnames = (t[0] for t in cursor.description)
    rows = cursor.fetchall()
    return pa.table(
        dict([_rows_to_column(rows, colname, i) for i, colname in enumerate(colnames)])
    )


def query_database(db: sqlite3.Connection) -> pa.Table:
    """Return a table; raise sqlite3.ProgrammingError if queries fail."""
    cursor = db.cursor()
    cursor.execute(REQUESTS_AND_CLAIMS_SQL)
    return _cursor_to_table(cursor)


class InvalidLz4File(Exception):
    """The file cannot be read as a .sqlite3.lz4 file."""


@contextlib.contextmanager
def _open_sqlite3_lz4_file(path) -> ContextManager[sqlite3.Connection]:
    with tempfile.NamedTemporaryFile(mode="wb") as tf:
        with lz4.frame.open(path) as lz4_file:
            while True:
                try:
                    block = lz4_file.read1()
                except Exception:
                    raise InvalidLz4File()
                if not block:
                    break
                tf.write(block)
        tf.flush()
        with sqlite3.connect(tf.name) as db:
            yield db


def render(arrow_table, params, output_path, **kwargs):
    if params["file"] is None:
        return []

    try:
        with _open_sqlite3_lz4_file(params["file"]) as db:
            validate_database(db)  # raises sqlite3.DatabaseError

            try:
                arrow_table = query_database(db)
            except sqlite3.ProgrammingError:
                return [i18n.trans("error.queryError", "Please upload a newer file.")]

            with pyarrow.RecordBatchFileWriter(str(output_path), arrow_table.schema) as writer:
                writer.write_table(arrow_table)
            return []
    except (InvalidLz4File, sqlite3.DatabaseError):
        return [i18n.trans(
            "error.invalidFile", "Please upload a valid .sqlite3.lz4 file."
        )]

import contextlib
import datetime
import json
import re
import sqlite3
import tempfile
from typing import Callable, ContextManager, List, NamedTuple, Optional, Tuple, Union

import dateutil.parser
import lz4.frame
import pyarrow as pa
import pyarrow.ipc
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

SUBMISSIONS_AND_CLAIMS_SQL = r"""
WITH
smooch_requests AS (
  SELECT
    json_extract(daf.value_json, '$.source.type')
      || ':'
      || json_extract(daf.value_json, '$.source.originalMessageId')
      AS submission_id,
    json_extract(daf.value_json, '$.authorId') AS submitted_by,
    a.created_at AS submitted_at,
    json_extract(daf.value_json, '$.text') AS request_text,
    a.annotated_id AS project_media_id
  FROM dynamic_annotation_fields daf
  INNER JOIN annotations a ON a.id = daf.annotation_id
  WHERE daf.field_name = 'smooch_data'
),
web_requests AS (
  SELECT
    'check:' || project_medias.id AS submission_id,
    'check:' || users.login as submitted_by,
    project_medias.created_at AS submitted_at,
    NULL AS request_text,
    project_medias.id AS project_media_id
  FROM project_medias
  LEFT JOIN users ON project_medias.user_id = users.id
  WHERE project_medias.id NOT IN (SELECT DISTINCT project_media_id FROM smooch_requests)
),
all_requests AS (
  SELECT * FROM smooch_requests
  UNION
  SELECT * FROM web_requests
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
  all_requests.submission_id AS "submission_id [text]",
  all_requests.submitted_by,
  all_requests.submitted_at,
  all_requests.request_text,
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
FROM all_requests
INNER JOIN project_medias ON project_medias.id = all_requests.project_media_id
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
ORDER BY all_requests.submitted_at DESC, project_medias.id
"""

ITEMS_SQL = r"""
WITH
statuses AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY a.annotated_id ORDER BY a.id DESC) AS rn_desc,
    daf.annotation_id,
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
    annotation_id,
    project_media_id,
    login,
    json_extract(value, '$') AS status -- it's a JSON-encoded String
  FROM statuses
  WHERE rn_desc = 1
),
last_analysis_titles AS (
  SELECT
    last_statuses.project_media_id,
    json_extract(daf.value, '$') AS title -- it's a JSON-encoded String
  FROM last_statuses
  INNER JOIN dynamic_annotation_fields daf
          ON daf.annotation_id = last_statuses.annotation_id
         AND daf.field_name = 'title'
),
last_analysis_contents AS (
  SELECT
    last_statuses.project_media_id,
    json_extract(daf.value, '$') AS content -- it's a JSON-encoded String
  FROM last_statuses
  INNER JOIN dynamic_annotation_fields daf
          ON daf.annotation_id = last_statuses.annotation_id
         AND daf.field_name = 'content'
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
  project_medias.id AS item_id,
  project_medias.created_at AS item_created_at,
  project_media_creators.login AS item_created_by,
  last_statuses.status AS item_status,
  CASE last_statuses.login WHEN 'smooch' THEN NULL ELSE last_statuses.login END AS item_status_by,
  last_analysis_titles.title AS item_analysis_title,
  last_analysis_contents.content AS item_analysis_content,
  project_media_list1s.list1 AS item_list1,
  COALESCE(
    json_extract(project_media_metadatas.metadata_json, '$.title'),
    json_extract(media_metadatas.metadata_json, '$.title')
  ) AS item_title,
  CASE medias.type WHEN 'Claim' THEN 'Text' ELSE medias.type END AS media_type,
  medias.url AS media_url,
  json_extract(media_metadatas.metadata_json, '$.published_at') AS media_published_at,
  json_extract(media_metadatas.metadata_json, '$.archives.archive_org.location') AS media_archive_org_url,
  json_extract(media_metadatas.metadata_json, '$.description') AS media_description,
  json_extract(media_metadatas.metadata_json, '$.author_name') AS media_author_name,
  json_extract(media_metadatas.metadata_json, '$.author_url') AS media_author_url,
  'https://checkmedia.org/' || teams.slug || '/media/' || project_medias.id AS check_url,
  parent_relationships.parent_project_media_id AS primary_item_id,
  parent_relationships.created_at AS primary_item_linked_at,
  parent_relationships.login AS primary_item_linked_by,
  first_status_change_events.created_at AS first_item_status_changed_at,
  first_status_change_events.login AS first_item_status_changed_by,
  last_status_change_events.created_at AS last_item_status_changed_at,
  last_status_change_events.login AS last_item_status_changed_by,
  last_reports.status AS item_report_status,
  first_publish_events.created_at AS item_report_first_published_at,
  first_publish_events.login AS item_report_first_published_by,
  project_medias.archived AS "item_archived [integer]",
  first_archived_events.created_at AS item_first_archived_at,
  first_archived_events.login AS item_first_archived_by
FROM project_medias
INNER JOIN medias ON project_medias.media_id = medias.id
INNER JOIN teams ON teams.id = project_medias.team_id
LEFT JOIN project_media_list1s ON project_media_list1s.project_media_id = project_medias.id
LEFT JOIN last_statuses ON last_statuses.project_media_id = project_medias.id
LEFT JOIN last_analysis_titles ON last_analysis_titles.project_media_id = project_medias.id
LEFT JOIN last_analysis_contents ON last_analysis_contents.project_media_id = project_medias.id
LEFT JOIN parent_relationships ON parent_relationships.child_project_media_id = project_medias.id
LEFT JOIN first_status_change_events ON first_status_change_events.project_media_id = project_medias.id
LEFT JOIN last_status_change_events ON last_status_change_events.project_media_id = project_medias.id
LEFT JOIN project_media_metadatas ON project_media_metadatas.project_media_id = project_medias.id
LEFT JOIN media_metadatas ON media_metadatas.media_id = medias.id
LEFT JOIN last_reports ON last_reports.project_media_id = project_medias.id
LEFT JOIN first_publish_events ON first_publish_events.project_media_id = project_medias.id
LEFT JOIN first_archived_events ON first_archived_events.project_media_id = project_medias.id
LEFT JOIN users project_media_creators ON project_media_creators.id = project_medias.user_id
ORDER BY project_medias.id DESC
"""

TASKS_SQL = r"""
CREATE INDEX annotations__annotated_id ON annotations (annotated_id);
CREATE INDEX dynamic_annotation_fields__annotation_id ON dynamic_annotation_fields (annotation_id);
SELECT
  annotations_tasks.id AS task_id,
  annotations_tasks.annotated_id AS item_id,
  annotations_tasks.created_at AS created_at,
  -- Parse YAML: half using user-defined function (UDF), half with LIKE.
  -- LIKE is wrong but fast. We know of no cases where "fieldset" will
  -- mis-parse.
  -- UDF is correct but slow: some tasks have newlines, so we know of cases
  -- where string manipulation would mis-parse.
  -- Our UDF is carefully optimized, and it's still slow. TODO convince our
  -- database designers to use DB fields instead of YAML.
  CASE
    WHEN annotations_tasks.data LIKE ('%' || CHAR(0xa) || 'fieldset: metadata' || CHAR(0xa) || '%') THEN 'metadata'
    ELSE 'task'
  END AS task_or_metadata,
  task_yaml_to_label(annotations_tasks.data) AS label,
  format_dynamic_annotation_field_value(
    dynamic_annotation_fields.annotation_type,
    dynamic_annotation_fields.field_type,
    CASE dynamic_annotation_fields.value
      WHEN '' THEN dynamic_annotation_fields.value_json
      ELSE dynamic_annotation_fields.value
    END
  ) AS answer,
  users.login AS answered_by,
  annotations_responses.created_at AS answered_at
FROM annotations annotations_tasks
LEFT JOIN annotations annotations_responses
       ON annotations_responses.annotated_type = 'Task'
      AND annotations_responses.annotated_id = annotations_tasks.id
      AND annotations_responses.annotation_type LIKE 'task_response_%'
LEFT JOIN dynamic_annotation_fields
       ON dynamic_annotation_fields.annotation_id = annotations_responses.id
LEFT JOIN users
       ON annotations_responses.annotator_type = 'User'
      AND annotations_responses.annotator_id = users.id
WHERE annotations_tasks.annotated_type = 'ProjectMedia'
  AND annotations_tasks.annotation_type = 'task'
ORDER BY
  annotations_tasks.annotated_id DESC,
  -- ... all ordered by label
  CASE
    WHEN annotations_tasks.data LIKE ('%' || CHAR(0xa) || 'fieldset: metadata' || CHAR(0xa) || '%') THEN 'metadata'
    ELSE 'task'
  END,
  task_yaml_to_label(annotations_tasks.data)
"""

CONVERSATIONS_SQL = r"""
WITH
smooch_users AS (
  SELECT
    json_extract(daf.value_json, '$.id') AS id,
    json_extract(daf.value_json, '$.raw.clients[0].platform') AS platform,
    -- There can be many annotations per WhatsApp user: one per project. We use
    -- MAX() to pick just one value per user (at random). (Assume non-NULL
    -- values are often identical or unique.)
    MAX(COALESCE(
        json_extract(daf.value_json, '$.raw.clients[0].externalId'), -- https://docs.smooch.io/rest/#client-schema
        json_extract(daf.value_json, '$.raw.clients[0].displayName'), -- WhatsApp
        json_extract(daf.value_json, '$.raw.clients[0].avatarUrl') -- fallback?
    )) AS user_id_on_platform, -- only tested on WhatsApp
    MAX(json_extract(slack_channel_urls.value, '$')) AS slack_channel_url
  FROM dynamic_annotation_fields daf
  INNER JOIN annotations ON daf.annotation_id = annotations.id
  LEFT JOIN dynamic_annotation_fields slack_channel_urls
         ON slack_channel_urls.annotation_id = annotations.id
        AND slack_channel_urls.field_name = 'smooch_user_slack_channel_url'
  WHERE daf.field_name = 'smooch_user_data'
  GROUP BY
    json_extract(daf.value_json, '$.id'),
    json_extract(daf.value_json, '$.raw.clients[0].platform')
)
SELECT
  json_extract(daf.value_json, '$.source.type')
    || ':'
    || json_extract(daf.value_json, '$.source.originalMessageId')
    AS "conversation_id [text]",
  smooch_users.platform || ':' || smooch_users.user_id_on_platform AS user,
  annotations.created_at AS created_at,
  CASE
    WHEN annotations.annotated_type = 'ProjectMedia' THEN CASE
      WHEN json_extract(daf.value_json, '$.project_id') IS NOT NULL THEN 'submission'
      ELSE 'resource'
    END
    ELSE NULL
  END AS outcome,
  CASE
    WHEN annotations.annotated_type = 'ProjectMedia' THEN annotations.annotated_id
    ELSE NULL
  END AS item_id,
  json_extract(daf.value_json, '$.text') AS user_messages, -- delimited by \u2063
  smooch_users.slack_channel_url AS slack_channel_url
FROM dynamic_annotation_fields daf
INNER JOIN annotations ON annotations.id = daf.annotation_id
LEFT JOIN smooch_users ON smooch_users.id = json_extract(daf.value_json, '$.authorId')
WHERE daf.field_name = 'smooch_data'
ORDER BY daf.created_at DESC, daf.id DESC
"""


def validate_database(db: sqlite3.Connection) -> None:
    """Raise sqlite3.DatabaseError if `db` does not point to a database."""
    db.execute("SELECT 1 FROM projects LIMIT 1")


class IntegerType:
    def list_to_pyarrow(self, values: List[Optional[int]]) -> pa.Array:
        return pa.array(values, pa.int32())  # TODO dynamic width?


class IdType(IntegerType):
    pass


class TextType:
    def list_to_pyarrow(self, values: List[Optional[str]]) -> pa.Array:
        return pa.array(values, pa.utf8())


class TimestampType:
    def list_to_pyarrow(self, values: List[Optional[str]]) -> pa.Array:
        def parse(v: Optional[str]) -> Optional[datetime.datetime]:
            if v is None:
                return None
            try:
                return dateutil.parser.isoparse(v)
            except ValueError:
                return None

        return pa.array([parse(v) for v in values], pa.timestamp("ns"))


QueryColumnType = Union[IdType, IntegerType, TextType, TimestampType]


class QueryColumn(NamedTuple):
    name: str
    query_column_type: QueryColumnType


def _column_name_to_query_column(name: str) -> QueryColumn:
    match = re.match(r"(.+) \[(id|integer|text|timestamp)\]", name)
    if match:
        name = match.group(1)
        if match.group(2) == "id":
            type = IdType()
        elif match.group(2) == "text":
            type = TextType()
        elif match.group(2) == "integer":
            type = IntegerType()
        elif match.group(2) == "timestamp":
            type = TimestampType()
        return QueryColumn(name, type)
    elif name.endswith("_id"):
        return QueryColumn(name, IdType())
    elif name.endswith("_at"):
        return QueryColumn(name, TimestampType())
    else:
        return QueryColumn(name, TextType())


def _rows_to_column(
    rows: list, column_name: str, column_index: int
) -> Tuple[str, pa.Array]:
    query_column = _column_name_to_query_column(column_name)
    values = list(r[column_index] for r in rows)
    return query_column.name, query_column.query_column_type.list_to_pyarrow(values)


def _cursor_to_table(cursor: sqlite3.Cursor) -> pa.Table:
    colnames = (t[0] for t in cursor.description)
    rows = cursor.fetchall()
    return pa.table(
        dict([_rows_to_column(rows, colname, i) for i, colname in enumerate(colnames)])
    )


def _query_submissions_and_claims(db: sqlite3.Connection) -> pa.Table:
    """Return a table; raise sqlite3.ProgrammingError if queries fail."""
    cursor = db.cursor()
    cursor.execute(SUBMISSIONS_AND_CLAIMS_SQL)
    return _cursor_to_table(cursor)


def _query_items(db: sqlite3.Connection) -> pa.Table:
    """Return a table; raise sqlite3.ProgrammingError if queries fail."""
    cursor = db.cursor()
    cursor.execute(ITEMS_SQL)
    return _cursor_to_table(cursor)


def _query_conversations(db: sqlite3.Connection) -> pa.Table:
    cursor = db.cursor()
    cursor.execute(CONVERSATIONS_SQL)
    table1 = _cursor_to_table(cursor)

    # Extract messages in Python, not SQLite UDF, so it's easy to debug the
    # query as described in the README.
    last_message_pattern = re.compile("(?:.*\n\u2063)*(.*)", re.DOTALL)

    def extract_last_message(messages_str: str) -> str:
        r"""Omit all but the final message from a conversation.

        Check doesn't use a JSON Array to delimit separate message texts.
        Instead, it delimits them by '\n\u2063'.
        """
        if messages_str is None:
            return None

        return last_message_pattern.match(messages_str).group(1)

    user_messages_list = table1["user_messages"].to_pylist()
    last_user_message_list = [extract_last_message(m) for m in user_messages_list]
    return table1.add_column(
        table1.column_names.index("user_messages") + 1,
        "last_user_message",
        pa.array(last_user_message_list, pa.utf8()),
    )


_DYNAMIC_DATETIME_FIELD_VALUE_REGEX = re.compile(
    (
        r'^"(?P<YYYY>\d{4})-(?P<MM>\d\d)-(?P<DD>\d\d) '  # YYYY-MM-DD
        r"(?P<h>\d\d?):(?P<m>\d\d?) "  # e.g., "0:1 " is 00:01 (12:01 AM)
        r"(?P<offset>[-+]?\d\d?) \w+ "  # e.g., "+3 EAT "
        r'(?P<notime>notime)?"$'  # either "notime" or ""
    ),
    re.ASCII,
)


def format_dynamic_annotation_field_value(
    annotation_type: str, field_type: str, value: str
) -> Optional[str]:
    """Format a dynamic value, very specific to Meedan.

    The decode logic was reverse-engineered by inspecting check-api and
    check-web source code. The formatting logic is custom here. The goal:
    make complex answers easy to read. Output may be ambiguous.
    (e.g., `["Option 1", "Option 2"]` may be formatted identically to
    `["Option 1; Option 2"]`.)
    """
    if field_type in ("text", "language", "json", "image_path", "id"):
        try:
            return str(json.loads(value))
        except ValueError:
            return value
    elif field_type == "select":
        try:
            decoded = json.loads(value)
        except ValueError:
            return value
        if (
            isinstance(decoded, str)
            and annotation_type == "task_response_multiple_choice"
        ):
            # Meedan double-encodes JSON. Decode AGAIN, if we can.
            try:
                decoded = json.loads(decoded)
            except ValueError:
                return decoded
            if (
                isinstance(decoded, dict)
                and "selected" in decoded
                and isinstance(decoded["selected"], list)
                and all(isinstance(s, str) for s in decoded["selected"])
            ):
                values = decoded["selected"]
                if isinstance(decoded.get("other"), str):
                    values += [f"Other ({decoded['other']})"]
                return ", ".join(values)
            return str(decoded)
        return str(decoded)
    elif field_type == "geojson":
        try:
            # Geojson is double-encoded. Decode it once.
            value_decoded_once = str(json.loads(value))
        except ValueError:
            return value
        try:
            value_decoded_twice = json.loads(value_decoded_once)  # raise ValueError
            if (
                isinstance(value_decoded_twice, dict)
                and value_decoded_twice.get("type") == "Feature"
                and isinstance(value_decoded_twice.get("geometry"), dict)
                and value_decoded_twice["geometry"].get("type") == "Point"
                and isinstance(value_decoded_twice["geometry"].get("coordinates"), list)
                and len(value_decoded_twice["geometry"]["coordinates"]) == 2
            ):
                lat, lng = value_decoded_twice["geometry"]["coordinates"]
                if not (
                    (isinstance(lat, int) or isinstance(lat, float))
                    and (isinstance(lng, int) or isinstance(lng, float))
                ):
                    raise ValueError("lat/lng are not both numbers")
                if isinstance(
                    value_decoded_twice.get("properties"), dict
                ) and isinstance(value_decoded_twice["properties"].get("name"), str):
                    name = value_decoded_twice["properties"]["name"]
                    return f"{name} ({lat}, {lng})"
                return f"({lat}, {lng})"
        except ValueError:
            return value_decoded_once
        return value_decoded_once
    elif field_type == "datetime":
        m = _DYNAMIC_DATETIME_FIELD_VALUE_REGEX.match(value)
        if m:
            if m.group("notime"):
                return "-".join((m.group("YYYY"), m.group("MM"), m.group("DD")))
            else:
                dt = (
                    datetime.datetime(
                        int(m.group("YYYY")),
                        int(m.group("MM")),
                        int(m.group("DD")),
                        int(m.group("h")),
                        int(m.group("m")),
                        0,
                        0,
                        datetime.timezone.utc,
                    )
                    - datetime.timedelta(hours=int(m.group("offset")))
                )
                # datetime.isoformat() returns ':SS' and misses 'Z', so we adjust it
                return dt.isoformat()[:16] + "Z"
        else:
            return value
    else:
        return value


def build_task_yaml_to_label() -> Callable[[str], str]:
    """Build a task_yaml_to_label() function, very specific to Meedan.

    It would be nicer if `label` were a database field and we could delete all
    this.
    """
    from yaml import reader, scanner, tokens, YAMLError

    class Scanner(reader.Reader, scanner.Scanner):
        def __init__(self, stream):
            reader.Reader.__init__(self, stream)
            scanner.Scanner.__init__(self)

    def task_yaml_to_label(task_yaml: str) -> Optional[str]:
        scanner = Scanner(task_yaml)
        # Heavily optimized
        nesting = 0
        # closeness_to_value meanings:
        # 0 = we're nowhere interesting
        # 1 = we saw a KeyToken (with nesting=1 [outer mapping])
        # 2 = ... and then a ScalarToken(value='label')
        # 3 = ... and then a ValueToken
        # ... so if the next token is a ScalarToken, we found our label!
        # Otherwise, reset closeness_to_value to 0 and keep scanning.
        closeness_to_value = 0
        try:
            while True:
                token = scanner.get_token()
                if token is None:
                    return None
                elif (
                    token.id is tokens.BlockMappingStartToken.id
                    or token.id is tokens.BlockSequenceStartToken.id
                    or token.id is tokens.FlowMappingStartToken.id
                    or token.id is tokens.FlowSequenceStartToken.id
                ):
                    nesting += 1
                    closeness_to_value = 0
                elif (
                    token.id is tokens.BlockEndToken.id
                    or token.id is tokens.FlowMappingEndToken.id
                    or token.id is tokens.FlowSequenceEndToken.id
                ):
                    nesting -= 1
                    closeness_to_value = 0
                elif nesting == 1:
                    if closeness_to_value == 0 and token.id is tokens.KeyToken.id:
                        closeness_to_value = 1
                    elif (
                        closeness_to_value == 1
                        and token.id is tokens.ScalarToken.id
                        and token.value == "label"
                    ):
                        closeness_to_value = 2
                    elif closeness_to_value == 2 and token.id is tokens.ValueToken.id:
                        closeness_to_value = 3
                    elif closeness_to_value == 3 and token.id is tokens.ScalarToken.id:
                        return str(token.value)
                    else:
                        closeness_to_value = 0
        except YAMLError:
            return None

    return task_yaml_to_label


def _query_tasks(db: sqlite3.Connection) -> pa.Table:
    db.create_function("task_yaml_to_label", 1, build_task_yaml_to_label())
    db.create_function(
        "format_dynamic_annotation_field_value",
        3,
        format_dynamic_annotation_field_value,
    )
    cursor = db.cursor()
    for sql in TASKS_SQL.split(";\n"):
        # This SQL creates indexes to speed up the full query. The final
        # query is the one _cursor_to_table() will iterate over.
        cursor.execute(sql)
    return _cursor_to_table(cursor)


def query_database(db: sqlite3.Connection, query_slug: str) -> pa.Table:
    """Return a table; raise sqlite3.ProgrammingError if queries fail."""
    if query_slug == "tasks":
        return _query_tasks(db)
    elif query_slug == "conversations":
        return _query_conversations(db)
    elif query_slug == "items":
        return _query_items(db)
    else:
        return _query_submissions_and_claims(db)


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
                arrow_table = query_database(db, params["query_slug"])
            except sqlite3.ProgrammingError:
                return [i18n.trans("error.queryError", "Please upload a newer file.")]

            with pyarrow.RecordBatchFileWriter(
                str(output_path), arrow_table.schema
            ) as writer:
                writer.write_table(arrow_table)
            return []
    except (InvalidLz4File, sqlite3.DatabaseError) as err:
        return [
            i18n.trans("error.invalidFile", "Please upload a valid .sqlite3.lz4 file.")
        ]


def _migrate_params_v0_to_v1(params):
    """v0: always `submissions_and_claims` query. v1: slugs.

    Valid slugs in v1:

    * submissions_and_claims
    * tasks [NOT FINALIZED]
    * conversations
    """
    return {
        **params,
        "query_slug": "submissions_and_claims",
    }


def migrate_params(params):
    if "query_slug" not in params:
        params = _migrate_params_v0_to_v1(params)
    return params

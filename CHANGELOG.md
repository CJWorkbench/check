2020-11-19.01
-------------

* Allow querying items even when the team has no verification statuses.
* Allow querying items whose status IDs are invalid.

2020-10-29.01
-------------

* Tasks: make "image" answers URLs

2020-10-28.01
-------------

* Tasks and metadata: add `first_note` column
* Items: add `item_notes` column (all notes, newline-separated)
* Items: add `item_language` column
* Nix "Submissions and Claims" query
* Dictionary-encode columns like "item_language". Should lead to smaller
  Parquet files (because columns can be encoded to be tiny).

2020-10-27.01
-------------

* Conversations: add `resource_title` column

2020-10-19.01
-------------

* Items: look up `item_status` label.
* [dev-mode] allow running from the command line.
* [internal] dictionary-encode username and status columns.

2020-10-15.01
-------------

* Items: add `item_analysis_title`, `item_analysis_content`
* Items: change `item_url` to `media_url` and `item_derived_***` to `media_***`
* Items: add `item_created_at`, `item_created_by`
* Items: add `item_tags`
* Tasks: hide JSON characters in image filenames, so they look nice

2020-10-14.01
-------------

* Tasks: hide JSON characters in multiple-choice task/metadata responses, so
  they look nice.
* Conversations: prevent duplicate rows.
* Items: add `item_derived_published_at`, `item_archive_org_url`,
  `item_derived_description`, `item_derived_author_name`,
  `item_derived_author_url`.

2020-10-13.01
-------------

* Tasks: Hide most JSON characters in task/metadata answers, so they look nice.

2020-09-25.01
-------------

* Add Conversations query
* Add Items query

2020-08-25.01
-------------

* Rename "Tasks", "Tasks and Metadata"

2020-08-17.01
-------------

* Add Tasks query

2020-07-28.01
-------------

* Initial deploy. "Requests and Claims" sheet, alpha version.

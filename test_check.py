from check import (
    build_task_yaml_to_label,
    migrate_params,
    format_dynamic_annotation_field_value,
)
import unittest


ANY_OLD_UUID = "f00198a1-c1b1-4f61-b125-de5df41b86a2"


class MigrateParamsTest(unittest.TestCase):
    def test_v0_to_v1(self):
        self.assertEqual(
            migrate_params({"file": ANY_OLD_UUID}),
            {"file": ANY_OLD_UUID, "query_slug": "submissions_and_claims"},
        )

    def test_v1(self):
        self.assertEqual(
            migrate_params({"file": ANY_OLD_UUID, "query_slug": "tasks"}),
            {"file": ANY_OLD_UUID, "query_slug": "tasks"},
        )


class TaskYamlToJsonTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.fn = build_task_yaml_to_label()

    def test_missing_value_is_none(self):
        self.assertIsNone(self.fn("label2: value2"))

    def test_happy_path(self):
        self.assertEqual(
            self.fn(
                r"""--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess
label: BOOM Link
type: free_text
options: []
description:
required: false
json_schema: '{"type":"string","pattern":"^https?://[^ ]+\.[^ ]+"}'
slug: boom_link
"""
            ),
            "BOOM Link",
        )

    def test_quoted_string(self):
        self.assertEqual(
            self.fn(
                r"""--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess
label: ","
type: free_text
required: false
description:
options: []
slug: ''
"""
            ),
            ",",
        )

    def test_skip_nested_things(self):
        self.assertEqual(
            self.fn(
                r"""--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess
options:
- { name: 'foo', value: 'bar' }
- name: foo
  value: bar
  label: foobar
  stuff: [1, 2, 3]
  more_nesting:
    label: foobarbaz
label: BOOM Link
"""
            ),
            "BOOM Link",
        )


class FormatDynamicAnnotationFieldValueTest(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.fn = format_dynamic_annotation_field_value

    def test_text_empty(self):
        self.assertEqual(self.fn("text", '""'), "")

    def test_text_json_decode(self):
        self.assertEqual(self.fn("text", r'"hi\"\nthere"'), 'hi"\nthere')

    def test_text_return_raw_on_error(self):
        self.assertEqual(self.fn("text", "foo"), "foo")  # not JSON? leave as-is

    def test_select_json_decode(self):
        self.assertEqual(self.fn("select", r'"more-information"'), "more-information")

    def test_select_return_raw_on_error(self):
        self.assertEqual(self.fn("select", r'"hi'), '"hi')

    def test_language_json_decode(self):
        self.assertEqual(self.fn("language", r'"vi"'), "vi")

    def test_language_return_raw_on_error(self):
        self.assertEqual(self.fn("language", r'"hi'), '"hi')

    def test_json_decode_double_encoded_json_once(self):
        self.assertEqual(
            self.fn("json", r'"{\"type\":\"text\",\"text\":\"👌\"}"'),
            '{"type":"text","text":"👌"}',
        )

    def test_image_path(self):
        self.assertEqual(
            self.fn("image_path", r'"https://example.org"'), "https://example.org"
        )

    def test_id(self):
        self.assertEqual(self.fn("id", '"1291428277.123212"'), "1291428277.123212")

    def test_geojson(self):
        self.assertEqual(
            self.fn(
                "geojson",
                r'"{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[45.4972159,-73.6103642]},\"properties\":{\"name\":\"Montreal, QC, Canada\"}}"',
            ),
            "Montreal, QC, Canada (45.4972159, -73.6103642)",
        )

    def test_geojson_no_name(self):
        self.assertEqual(
            self.fn(
                "geojson",
                r'"{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[45.4972159,-73.6103642]},\"properties\":{}}"',
            ),
            "(45.4972159, -73.6103642)",
        )

    def test_geojson_name_not_str(self):
        self.assertEqual(
            self.fn(
                "geojson",
                r'"{\"type\":\"Feature\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[45.4972159,-73.6103642]},\"properties\":{\"name\":{}}}"',
            ),
            "(45.4972159, -73.6103642)",
        )

    def test_geojson_type_not_feature(self):
        self.assertEqual(
            self.fn(
                "geojson",
                r'"{\"type\":\"Point\",\"coordinates\":[45.4972159,-73.6103642]}"',
            ),
            r'{"type":"Point","coordinates":[45.4972159,-73.6103642]}',
        )

    def test_datetime_gmt(self):
        self.assertEqual(
            self.fn("datetime", '"2020-01-28 01:11 0 GMT "'), "2020-01-28T01:11Z"
        )

    def test_datetime_not_gmt(self):
        self.assertEqual(
            self.fn("datetime", '"2019-07-22 11:40 +3 EAT "'), "2019-07-22T08:40Z"
        )

    def test_datetime_change_days_with_timezone(self):
        self.assertEqual(
            self.fn("datetime", '"2019-07-22 1:40 +3 EAT "'), "2019-07-21T22:40Z"
        )

    def test_datetime_notime(self):
        self.assertEqual(
            self.fn("datetime", '"2020-03-01 0:0 0 GMT notime"'), "2020-03-01"
        )

    def test_datetime_error_is_raw(self):
        self.assertEqual(self.fn("datetime", '"200'), '"200')

    def test_boolean_true(self):
        self.assertEqual(self.fn("boolean", "true"), "true")

    def test_boolean_false(self):
        self.assertEqual(self.fn("boolean", "false"), "false")


if __name__ == "__main__":
    unittest.main()

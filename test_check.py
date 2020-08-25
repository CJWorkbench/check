from check import migrate_params, build_task_yaml_to_label
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
                """--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess
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
                """--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess
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
                """--- !ruby/hash:ActiveSupport::HashWithIndifferentAccess
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


if __name__ == "__main__":
    unittest.main()

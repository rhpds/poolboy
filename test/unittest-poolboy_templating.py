#!/usr/bin/env python3

import unittest
import sys
sys.path.append('../operator')

from poolboy_templating import recursive_process_template_strings, seconds_to_interval

class TestJsonPatch(unittest.TestCase):
    def test_00(self):
        template = {}
        variables = {}
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {}
        )

    def test_01(self):
        template = {
            "a": "b",
            "b": "2",
            "c": 3,
            "d": {
                "e": ["a", "b", "c"]
            }
        }
        variables = {}
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            template
        )

    def test_02(self):
        template = {
            "a": "{{ foo }}",
        }
        variables = {
            "foo": "bar"
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": "bar"
            }
        )

    def test_03(self):
        template = {
            "a": ["{{ foo }}"],
        }
        variables = {
            "foo": "bar"
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": ["bar"]
            }
        )

    def test_04(self):
        template = {
            "a": "{{ foo }}"
        }
        variables = {
            "foo": True
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": "True"
            }
        )

    def test_05(self):
        template = {
            "a": "{{ foo | bool }}"
        }
        variables = {
            "foo": True
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": True
            }
        )

    def test_06(self):
        template = {
            "a": "{{ numerator / denominator }}"
        }
        variables = {
            "numerator": 1,
            "denominator": 2,
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": "0.5"
            }
        )

    def test_07(self):
        template = {
            "a": "{{ (numerator / denominator) | float }}"
        }
        variables = {
            "numerator": 1,
            "denominator": 2,
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": 0.5
            }
        )

    def test_08(self):
        template = {
            "a": "{{ n }}"
        }
        variables = {
            "n": 21,
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": "21"
            }
        )

    def test_09(self):
        template = {
            "a": "{{ n | int }}"
        }
        variables = {
            "n": 21
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "a": 21
            }
        )

    def test_10(self):
        template = {
            "user": "{{ user }}"
        }
        variables = {
            "user": {
                "name": "alice"
            }
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "user": "{'name': 'alice'}"
            }
        )

    def test_11(self):
        template = {
            "user": "{{ user | object }}"
        }
        variables = {
            "user": {
                "name": "alice"
            }
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables),
            {
                "user": {
                    "name": "alice"
                }
            }
        )

    def test_12(self):
        template = {
            "now": "{{ now() }}"
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertRegex(template_out['now'], r'^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+$')

    def test_13(self):
        template = {
            "now": "{{ now(True, '%FT%TZ') }}"
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertRegex(template_out['now'], r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$')

    def test_14(self):
        template = {
            "ts": "{{ (now() + timedelta(hours=3)).strftime('%FT%TZ') }}"
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertRegex(template_out['ts'], r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$')

    def test_15(self):
        template = {
            "ts": "{{ (now() + timedelta(hours=3)).strftime('%FT%TZ') }}"
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertRegex(template_out['ts'], r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$')

    def test_16(self):
        template = {
            "ts": "{{ (datetime.now(timezone.utc) + timedelta(hours=3)).strftime('%FT%TZ') }}"
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertRegex(template_out['ts'], r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$')

    def test_17(self):
        template = {
            "ts": "{{ (datetime.now(timezone.utc) + '3h' | parse_time_interval).strftime('%FT%TZ') }}"
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertRegex(template_out['ts'], r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$')

    def test_18(self):
        template = {
            "ts1": "{{ timestamp.add('3h') }}",
            "ts2": "{{ (datetime.now(timezone.utc) + timedelta(hours=3)).strftime('%FT%TZ') }}"
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertRegex(template_out['ts1'], r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ$')
        self.assertEqual(template_out['ts1'], template_out['ts2'])

    def test_19(self):
        template = {
            "ts": "{{ timestamp('1970-01-01T01:02:03Z').add('3h') }}",
        }
        variables = {}
        template_out = recursive_process_template_strings(template, variables)
        self.assertEqual(template_out['ts'], '1970-01-01T04:02:03Z')

    def test_20(self):
        template = "{{ user | json_query('name') }}"
        variables = {
            "user": {
                "name": "alice"
            }
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), "alice"
        )

    def test_21(self):
        template = "{{ users | json_query('[].name') | object }}"
        variables = {
            "users": [
                {
                    "name": "alice",
                    "age": 120,
                }, {
                    "name": "bob",
                    "age": 100,
                }
            ]
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), ["alice", "bob"]
        )

    # Test complicated case used to determine desired state in babylon governor
    def test_22(self):
        template = """
        {%- if 0 < resource_states | map('default', {}, True) | list | json_query("length([?!contains(keys(status.towerJobs.provision || `{}`), 'completeTimestamp')])") -%}
        {#- desired_state started until all AnarchySubjects have finished provision -#}
        started
        {%- elif 0 < resource_templates | json_query("length([?spec.vars.action_schedule.start < '" ~ now(True, "%FT%TZ") ~ "' && spec.vars.action_schedule.stop > '" ~ now(True, "%FT%TZ") ~ "'])") -%}
        {#- desired_state started for all if any should be started as determined by action schedule -#}
        started
        {%- elif 0 < resource_templates | json_query("length([?spec.vars.default_desired_state == 'started' && !(spec.vars.action_schedule.start || spec.vars.action_schedule.stop)])") -%}
        {#- desired_state started for all if any should be started as determined by default_desired_state -#}
        started
        {%- else -%}
        stopped
        {%- endif -%}
        """
        variables = {
            "resource_states": [
                {
                    "status": {
                        "towerJobs": {
                            "provision": {
                                "completeTimestamp": "2022-01-01T00:00:00Z"
                            }
                        }
                    }
                },
                {
                    "status": {
                        "towerJobs": {
                            "provision": {
                                "completeTimestamp": "2022-01-01T00:00:00Z"
                            }
                        }
                    }
                }
            ],
            "resource_templates": [
                {
                    "spec": {
                        "vars": {
                            "action_schedule": {
                                "start": "2022-01-01T00:00:00",
                                "stop": "2022-01-01T00:00:01",
                            }
                        }
                    }
                },
                {
                    "spec": {
                        "vars": {
                            "action_schedule": {
                                "start": "2022-01-01T00:00:00",
                                "stop": "2022-01-01T00:00:01",
                            }
                        }
                    }
                }
            ]
        }

        self.assertEqual(
            recursive_process_template_strings(template, variables), "stopped"
        )

        variables['resource_templates'][0]['spec']['vars']['action_schedule']['stop'] = '2099-12-31T23:59:59Z'
        self.assertEqual(
            recursive_process_template_strings(template, variables), "started"
        )

        variables['resource_templates'][0]['spec']['vars']['action_schedule']['stop'] = '2022-01-01T00:00:00Z'
        del variables['resource_states'][1]['status']['towerJobs']['provision']['completeTimestamp']
        self.assertEqual(
            recursive_process_template_strings(template, variables), "started"
        )

        del variables['resource_states'][1]['status']['towerJobs']['provision']
        self.assertEqual(
            recursive_process_template_strings(template, variables), "started"
        )

    def test_23(self):
        self.assertEqual(
            seconds_to_interval(seconds=600.0),
            "10m",
        )

    def test_24(self):
        template = {
            "a": "A",
            "b": "{{ omit }}",
        }
        variables = {}
        self.assertEqual(
            recursive_process_template_strings(template, variables), {"a": "A"}
        )

    def test_25(self):
        template = [
            "a", "{{ omit }}", "b"
        ]
        variables = {}
        self.assertEqual(
            recursive_process_template_strings(template, variables), ["a", "b"]
        )

    def test_26(self):
        template = {
            "a": "{{ a | default(omit) }}",
            "b": "{{ b | default(omit) }}",
        }
        variables = {
            "a": "A",
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), {"a": "A"}
        )

    def test_27(self):
        template = "{{ l | json_query('from_items(zip([].keys(@)[], [].values(@)[]))') | object }}"
        variables = {
            "l": [{"a": "A", "b": "X"}, {"b": "B", "c": "C"}, {"d": "D"}]
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), {"a": "A", "b": "B", "c": "C", "d": "D"}
        )

    def test_28(self):
        template = "{{ l | merge_list_of_dicts | object }}"
        variables = {
            "l": [{"a": "A", "b": "X"}, {"b": "B", "c": "C"}, {"d": "D"}]
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), {"a": "A", "b": "B", "c": "C", "d": "D"}
        )

    def test_29(self):
        template = "{{ l | merge_list_of_dicts | object }}"
        variables = {
            "l": [{"a": "A", "b": "X"}, None, {"b": "B", "c": "C"}, {}, {"d": "D"}]
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), {"a": "A", "b": "B", "c": "C", "d": "D"}
        )

    def test_30(self):
        template = "{{ l | merge_list_of_dicts | object }}"
        variables = {
            "l": []
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), {}
        )

    def test_31(self):
        template = "{{ l | merge_list_of_dicts | object }}"
        variables = {
            "l": [{"a": "A"}]
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), {"a": "A"}
        )

    def test_31(self):
        template = "{{ l | merge_list_of_dicts | object }}"
        variables = {
            "l": [{"a": "A"}]
        }
        self.assertEqual(
            recursive_process_template_strings(template, variables), {"a": "A"}
        )

    def test_32(self):
        template = "{{ a }}"
        template_variables = {
            "a": "{{ b }}",
            "b": "A",
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            "A"
        )

    def test_33(self):
        template = "{{ a | int }}"
        template_variables = {
            "a": "{{ b }}",
            "b": 2,
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            2
        )

    def test_34(self):
        template = "{{ a }}"
        template_variables = {
            "a": "{{ a }}",
        }
        with self.assertRaises(Exception):
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),

    def test_35(self):
        template = "{{ a | bool }}"
        template_variables = {
            "a": "{{ b }}",
            "b": False,
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            False
        )

    def test_36(self):
        template = "{{ a | int }}"
        template_variables = {
            "a": "{{ b.a }}",
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            23
        )

    def test_37(self):
        template = "{{ a.a | int }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            23
        )

    def test_38(self):
        template = "{{ (a.a > 3) | bool }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_39(self):
        template = "{{ (a.a >= 23) | bool }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_40(self):
        template = "{{ (a.a < 100) | bool }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_41(self):
        template = "{{ (a.a <= 23) | bool }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_42(self):
        template = "{{ (a.a == 23) | bool }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_43(self):
        template = "{{ (a['a'] == 23) | bool }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_44(self):
        template = "{{ ('a' in a) | bool }}"
        template_variables = {
            "a": '{{ {"a": b.a} | object }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_45(self):
        template = "{{ ('a' in a) | bool }}"
        template_variables = {
            "a": '{{ b.keys() | list }}',
            "b": {"a": 23},
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            True
        )

    def test_46(self):
        template = "{% if show_a %}a{% endif %}"
        template_variables = {
            "show_a": True,
        }
        self.assertEqual(
            recursive_process_template_strings(
                template, template_variables=template_variables
            ),
            "a",
        )

if __name__ == '__main__':
    unittest.main()

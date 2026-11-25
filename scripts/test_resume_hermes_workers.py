"""No real Herdr commands: uv run python -m unittest discover -s scripts."""

import unittest
from unittest.mock import patch

import resume_hermes_workers as resume


WORKER = {"name": "hermes-test", "session_id": "saved-id", "session_file": "/saved.jsonl"}
TAB = {"workspace_id": "w1B", "tab_id": "tab", "label": WORKER["name"]}
PANE = {"workspace_id": "w1B", "tab_id": "tab", "pane_id": "pane"}
AGENT = {**PANE, "agent": "pi", "name": WORKER["name"],
         "agent_session": {"value": WORKER["session_file"]}}


class RestoreTests(unittest.TestCase):
    def plan(self, agents=(), tabs=(), panes=()):
        return resume.make_plan([WORKER], "w1B", agents, tabs, panes)

    def test_live_exact_session_is_not_prompted_or_restarted(self):
        with patch.object(resume, "herdr") as api:
            resume.restore(self.plan([AGENT]), "w1B")
            api.assert_not_called()

    def test_foreign_workspace_session_or_name_is_not_touched(self):
        for agent in ({**AGENT, "workspace_id": "elsewhere"},
                      {**AGENT, "agent_session": {"value": "/different.jsonl"}}):
            with self.assertRaises(RuntimeError):
                self.plan([agent])

    def test_only_destination_workspace_tab_can_be_reused(self):
        unrelated = {**TAB, "workspace_id": "elsewhere"}
        self.assertEqual(self.plan(tabs=[unrelated])[0][1], "create")
        with patch.object(resume, "herdr", return_value={}) as api:
            resume.restore(self.plan(tabs=[TAB], panes=[PANE]), "w1B")
            api.assert_called_once_with(
                "agent", "start", WORKER["name"], "--kind", "pi", "--pane", "pane",
                "--", "--session", WORKER["session_file"], "--presets:preset", "eng")

    def test_new_tab_uses_returned_pane_no_focus_exact_session_no_prompt(self):
        with patch.object(resume, "herdr", side_effect=[
            {"root_pane": {"pane_id": "new-pane"}}, {},
        ]) as api:
            resume.restore(self.plan(), "w1B")
            self.assertEqual(api.call_count, 2)
            api.assert_any_call("tab", "create", "--workspace", "w1B", "--cwd",
                                str(resume.REPO), "--label", WORKER["name"], "--no-focus")
            self.assertEqual(api.call_args.args, (
                "agent", "start", WORKER["name"], "--kind", "pi", "--pane", "new-pane",
                "--", "--session", WORKER["session_file"], "--presets:preset", "eng"))

    def test_dry_run_never_mutates(self):
        with patch.object(resume, "herdr") as api:
            resume.restore(self.plan(), "w1B", dry_run=True)
            resume.restore(self.plan(tabs=[TAB], panes=[PANE]), "w1B", dry_run=True)
            api.assert_not_called()

    def test_occupied_split_or_duplicate_tabs_refused(self):
        for tabs, panes in [([TAB], [{**PANE, "agent": "pi"}]),
                            ([TAB], [PANE, {**PANE, "pane_id": "second"}]),
                            ([TAB, TAB], [])]:
            with self.assertRaises(RuntimeError):
                self.plan(tabs=tabs, panes=panes)

    def test_failure_stops_without_retry(self):
        with patch.object(resume, "herdr", side_effect=RuntimeError("uncertain")) as api:
            with self.assertRaises(RuntimeError):
                resume.restore([(WORKER, "create", None)], "w1B")
            self.assertEqual(api.call_count, 1)

    def test_destination_can_differ_from_catalogued_workspace(self):
        with patch.dict(resume.os.environ, {"HERDR_ENV": "1", "HERDR_WORKSPACE_ID": "new-workspace"}), \
             patch.object(resume.sys, "argv", ["resume"]), \
             patch.object(resume, "validate_sessions"), \
             patch.object(resume, "herdr", side_effect=[
                 {"agents": []}, {"tabs": []}, {"panes": []},
             ]) as api, \
             patch.object(resume, "restore") as restore:
            resume.main()
            api.assert_any_call("tab", "list", "--workspace", "new-workspace")
            api.assert_any_call("pane", "list", "--workspace", "new-workspace")
            self.assertEqual(restore.call_args.args[1], "new-workspace")
            self.assertTrue(all(action == "create" for _, action, _ in restore.call_args.args[0]))

    def test_outside_herdr_does_not_read_or_control_sessions(self):
        with patch.dict(resume.os.environ, {"HERDR_ENV": "0", "HERDR_WORKSPACE_ID": ""}), \
             patch.object(resume.sys, "argv", ["resume"]), \
             patch.object(resume, "validate_sessions") as validate, \
             patch.object(resume, "herdr") as api:
            with self.assertRaises(RuntimeError):
                resume.main()
            validate.assert_not_called()
            api.assert_not_called()


if __name__ == "__main__":
    unittest.main()

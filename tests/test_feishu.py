import json
import unittest

import httpx

from callarchive.notifications import Delivery, Message
from callarchive.notifications.feishu import FeishuChannel, build_card, summary_line


class FeishuTest(unittest.TestCase):
    def test_single_alert_links_the_number_inline(self):
        content = {
            "kind": "alert", "details_url": "https://archive.example/calls/pending?call=abc(2)",
            "issue": {"category": "follow_up", "callee": "13000000001", "reason": "archive_target_missing",
                      "started_at": "2026-11-24T08:06:20+08:00", "recording_duration_ms": 92000},
        }
        card = build_card(Message("single", "operator", "待跟进", content))
        self.assertEqual(card["header"]["template"], "blue")
        self.assertEqual(card["config"]["style"]["color"]["cus-0"],
                         {"light_mode": "rgba(204,32,1,1.000000)", "dark_mode": "rgba(204,32,1,1.000000)"})
        summary, action = card["body"]["elements"]
        self.assertEqual(summary, {"tag": "markdown", "content":
            "[13000000001](https://archive.example/calls/pending?call=abc%282%29) · 11/24 08:06 · "
            "⏱\ufe0e <font color='cus-0'>01:32</font>"})
        self.assertEqual(action["tag"], "div")
        without_url = build_card(Message("single", "operator", "待跟进", {**content, "details_url": None}))
        self.assertNotIn("https://", without_url["body"]["elements"][0]["content"])
        admin = build_card(Message("admin", "operator", "管理员事项", {
            **content, "issue": {**content["issue"], "category": "admin"},
        }))
        self.assertEqual(admin["header"]["template"], "orange")

    def test_duration_color_matches_short_call_boundary(self):
        short = summary_line({"callee": "10001", "recording_duration_ms": 119999})
        regular = summary_line({"callee": "10002", "recording_duration_ms": 120000})
        self.assertIn("<font color='cus-0'>01:59</font>", short)
        self.assertEqual(regular, "10002 · ⏱\ufe0e 02:00")
        card = build_card(Message("daily", "operator", "日报", {
            "issues": [{"category": "follow_up", "callee": "10001",
                        "reason": "archive_target_missing", "recording_duration_ms": 119999}],
        }))
        detail = card["body"]["elements"][0]["elements"][1]["content"]
        self.assertIn("⏱\ufe0e <font color='cus-0'>01:59</font>", detail)
        self.assertEqual(card["config"]["style"]["color"]["cus-0"]["light_mode"], "rgba(204,32,1,1.000000)")

    def test_collapsed_card_and_explicit_receipts(self):
        def respond(request):
            payload = json.loads(request.content)
            self.assertEqual(payload["msg_type"], "interactive")
            self.assertEqual(payload["card"]["schema"], "2.0")
            self.assertEqual(payload["timestamp"], "100")
            self.assertTrue(payload["sign"])
            panels = [element for element in payload["card"]["body"]["elements"]
                      if element["tag"] == "collapsible_panel"]
            self.assertEqual(len(panels), 1)
            self.assertFalse(panels[0]["expanded"])
            self.assertIn("10002", str(panels[0]))
            button = panels[0]["elements"][-1]["columns"][0]["elements"][0]
            self.assertEqual(button["width"], "fill")
            self.assertEqual(button["behaviors"][0]["default_url"],
                             "https://archive.example/workbench")
            return responses.pop(0)

        responses = [httpx.Response(200, json={"code": 0}),
                     httpx.Response(200, json={"code": 19021}),
                     httpx.Response(503, text="fail"),
                     httpx.Response(429), httpx.Response(200, json={"code": 11232})]
        with httpx.Client(transport=httpx.MockTransport(respond)) as http:
            channel = FeishuChannel(http, "https://open.feishu.cn/open-apis/bot/v2/hook/example",
                                    secret="secret", clock=lambda: 100)
            message = Message("test", "operator", "通话归档日报", {
                "page": 1, "total_pages": 1, "waiting_recordings": 2, "total_issues": 1,
                "workbench_url": "https://archive.example/workbench",
                "issues": [{"category": "admin", "callee": "10002",
                            "started_at": "2026-11-24T08:06:20+08:00",
                            "reason": "attachment_write_failed"}],
            })
            self.assertFalse(channel.supports_idempotency)
            self.assertEqual(channel.send(message).status, Delivery.CONFIRMED)
            self.assertEqual(channel.send(message).status, Delivery.REJECTED)
            self.assertEqual(channel.send(message).status, Delivery.UNKNOWN)
            for reference in ("http_429", "11232"):
                receipt = channel.send(message)
                self.assertEqual((receipt.status, receipt.reference), (Delivery.REJECTED, reference))
                self.assertIn(receipt.reference, channel.retryable_rejections)

#!/usr/bin/env python3
"""Hermetic syntax and metric-contract checks for Mino Prometheus rules."""

import json
import os
import re
import unittest
from pathlib import Path


def runfile(path: str) -> Path:
    return Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"] / path


TOKEN = re.compile(
    r'\s*(?:((?:(?:mino_[a-zA-Z0-9_:]+)|(?:up\{job="mino"\}))(?:\[[0-9]+[smhd]\])?)|'
    r"([0-9]+(?:\.[0-9]+)?)|(==|!=|>=|<=|>|<|\+|-|\*|/)|"
    r"([A-Za-z_][A-Za-z0-9_]*)|(\()|(\))|(,))"
)


class PromQlSubsetParser:
    """Parser for the intentionally small alert-expression subset in this file."""

    def __init__(self, expression: str):
        self.tokens = []
        cursor = 0
        while cursor < len(expression):
            match = TOKEN.match(expression, cursor)
            if match is None:
                raise ValueError(f"invalid PromQL at offset {cursor}: {expression}")
            self.tokens.append(next(value for value in match.groups() if value))
            cursor = match.end()
        self.position = 0

    def parse(self):
        self._binary(0)
        if self.position != len(self.tokens):
            raise ValueError(f"unexpected token {self.tokens[self.position]}")

    def _binary(self, minimum_precedence: int):
        self._primary()
        precedence = {
            "and": 1,
            "==": 2,
            "!=": 2,
            ">=": 2,
            "<=": 2,
            ">": 2,
            "<": 2,
            "+": 3,
            "-": 3,
            "*": 4,
            "/": 4,
        }
        while self.position < len(self.tokens):
            operator = self.tokens[self.position]
            current = precedence.get(operator, -1)
            if current < minimum_precedence:
                return
            self.position += 1
            self._binary(current + 1)

    def _primary(self):
        if self.position >= len(self.tokens):
            raise ValueError("expression ended before an operand")
        token = self.tokens[self.position]
        self.position += 1
        if token == "(":
            self._binary(0)
            self._expect(")")
            return
        if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", token):
            return
        if token.startswith("mino_") or token == 'up{job="mino"}':
            return
        if token not in {"increase", "rate", "time"}:
            raise ValueError(f"unsupported function or identifier {token}")
        self._expect("(")
        if token != "time":
            self._binary(0)
        self._expect(")")

    def _expect(self, expected: str):
        if self.position >= len(self.tokens) or self.tokens[self.position] != expected:
            raise ValueError(f"expected {expected}")
        self.position += 1


class AlertRulesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rules_text = runfile("configs/alerts/mino.rules.yml").read_text()
        # JSON is a YAML 1.2 subset, making this a deterministic syntax check
        # without a non-hermetic PyYAML dependency.
        cls.document = json.loads(cls.rules_text)
        cls.metric_source = runfile(
            "mino/observability/operational_metrics.cc"
        ).read_text()

    def test_rule_schema_promql_and_runbooks(self):
        self.assertEqual(set(self.document), {"groups"})
        names = set()
        components = set()
        registered = set(re.findall(r'"(mino_[a-zA-Z0-9_:]+)"', self.metric_source))
        referenced = set()
        for group in self.document["groups"]:
            self.assertRegex(group["name"], r"^[a-z][a-z0-9-]+$")
            self.assertRegex(group["interval"], r"^[1-9][0-9]*[smh]$")
            for rule in group["rules"]:
                self.assertNotIn(rule["alert"], names)
                names.add(rule["alert"])
                PromQlSubsetParser(rule["expr"]).parse()
                self.assertTrue(
                    all(selector == '{job="mino"}' for selector in re.findall(r"\{[^}]*\}", rule["expr"])),
                    "only the fixed low-cardinality Prometheus job selector is allowed",
                )
                referenced.update(
                    metric.split("[", 1)[0]
                    for metric in re.findall(r"mino_[a-zA-Z0-9_:]+(?:\[[0-9]+[smhd]\])?", rule["expr"])
                )
                self.assertRegex(rule["for"], r"^[0-9]+[smh]$")
                self.assertIn(rule["labels"]["severity"], {"warning", "critical"})
                components.add(rule["labels"]["component"])
                self.assertTrue(rule["annotations"]["summary"])
                self.assertRegex(
                    rule["annotations"]["runbook_url"],
                    r"/docs/operations/monitoring\.md#[a-z0-9-]+$",
                )
        self.assertEqual(referenced - registered, set(), "rules reference unregistered metrics")
        self.assertEqual(
            components,
            {"queue", "slab", "lease", "bridge", "storage", "exporter", "capacity", "tls_acl"},
        )

    def test_no_high_cardinality_identity_labels(self):
        forbidden = {"node", "node_id", "topic", "topic_id", "certificate", "cert", "peer"}
        for group in self.document["groups"]:
            for rule in group["rules"]:
                self.assertTrue(forbidden.isdisjoint(rule["labels"]))
        self.assertNotRegex(self.metric_source, r"\{(?:node|topic|cert|peer)[^}]*\}")


if __name__ == "__main__":
    unittest.main()

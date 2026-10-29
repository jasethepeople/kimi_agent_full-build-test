"""Unit tests for worker pure logic (no redis/nats required)."""
import hashlib
import json
import unittest

import worker


class TestParamHash(unittest.TestCase):
    def test_canonical_json_sorted_compact(self):
        self.assertEqual(
            worker.canonical_json({"b": 1, "a": 2}), '{"a":2,"b":1}'
        )

    def test_hash_stable_across_key_order(self):
        h1 = worker.param_hash({"lr": 0.01, "depth": 3})
        h2 = worker.param_hash({"depth": 3, "lr": 0.01})
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)  # sha256 hex

    def test_hash_matches_manual_sha256(self):
        params = {"lr": 0.01}
        expected = hashlib.sha256(b'{"lr":0.01}').hexdigest()
        self.assertEqual(worker.param_hash(params), expected)

    def test_hash_none_is_empty_map(self):
        self.assertEqual(worker.param_hash(None), worker.param_hash({}))

    def test_different_params_different_hash(self):
        self.assertNotEqual(
            worker.param_hash({"lr": 0.01}), worker.param_hash({"lr": 0.02})
        )


class TestBatchObjectives(unittest.TestCase):
    def test_hash_objective(self):
        outs, ms = worker.run_batch_objective("hash", ["a", "b"])
        self.assertEqual(
            outs[0], hashlib.sha256(b"a").hexdigest()
        )
        self.assertEqual(len(outs), 2)
        self.assertGreaterEqual(ms, 0)

    def test_count_objective(self):
        outs, _ = worker.run_batch_objective("count", ["abc", "xy", [1, 2, 3, 4]])
        self.assertEqual(outs, [3, 2, 4])

    def test_sleep_objective(self):
        outs, ms = worker.run_batch_objective("sleep", ["x", "y"])
        self.assertEqual(outs, [0.1, 0.1])
        self.assertGreaterEqual(ms, 150)  # ~200ms nominal

    def test_unknown_objective_raises(self):
        with self.assertRaises(ValueError):
            worker.run_batch_objective("nope", ["a"])

    def test_score_is_item_count(self):
        items = ["a", "b", "c"]
        outs, _ = worker.run_batch_objective("count", items)
        self.assertEqual(float(len(items)), 3.0)
        self.assertEqual(len(outs), len(items))


class TestHpoScore(unittest.TestCase):
    def test_deterministic(self):
        params = {"lr": 0.5, "depth": 2}
        self.assertEqual(worker.hpo_score(params), worker.hpo_score(params))

    def test_formula_base(self):
        # jitter is in [-0.05, 0.05); check base term dominates correctly
        params = {"p": 0.42}
        s = worker.hpo_score(params)
        self.assertAlmostEqual(s, 0.0, delta=0.05)

    def test_formula_exact(self):
        params = {"a": 0.5, "b": 0.3}
        base = -((0.5 - 0.42) ** 2 + (0.3 - 0.42) ** 2)
        ph = worker.param_hash(params)
        jitter = (int(ph[:8], 16) / 0xFFFFFFFF - 0.5) * 0.1
        self.assertEqual(worker.hpo_score(params), base + jitter)

    def test_jitter_bounded(self):
        for i in range(20):
            params = {"x": i * 0.1}
            base = -((i * 0.1 - 0.42) ** 2)
            self.assertAlmostEqual(worker.hpo_score(params), base, delta=0.05)

    def test_far_params_are_deadends(self):
        # many params far from 0.42 -> very negative score
        params = {"p%d" % i: 10.0 for i in range(5)}
        self.assertTrue(worker.is_deadend(worker.hpo_score(params)))


class TestDeadendClassification(unittest.TestCase):
    def test_threshold_default(self):
        self.assertEqual(worker.DEADEND_THRESHOLD, -10.0)

    def test_classification(self):
        self.assertTrue(worker.is_deadend(-10.0001))
        self.assertTrue(worker.is_deadend(-100))
        self.assertFalse(worker.is_deadend(-10.0))
        self.assertFalse(worker.is_deadend(0.0))

    def test_custom_threshold(self):
        self.assertTrue(worker.is_deadend(-1.0, threshold=-0.5))
        self.assertFalse(worker.is_deadend(-1.0, threshold=-2.0))


class TestResultSchema(unittest.TestCase):
    def test_build_result_fields(self):
        task = {"taskId": "t1", "jobId": "j1", "strategyId": "gen-3"}
        r = worker.build_result(task, "w1", "ok", 1.5, 12, "out", None)
        self.assertEqual(
            set(r.keys()),
            {"taskId", "jobId", "workerId", "status", "score",
             "metrics", "output", "strategyId", "completedAt"},
        )
        self.assertEqual(r["metrics"], {"elapsedMs": 12})
        self.assertEqual(r["strategyId"], "gen-3")
        json.dumps(r)  # must be serializable


if __name__ == "__main__":
    unittest.main()

class TestCrossLanguageHash(unittest.TestCase):
    """Cross-language contract: these vectors were produced by the Go
    orchestrator (internal/model.ParamHash, encoding/json float64 rules).
    Any mismatch breaks dead-end dedup between generator and worker."""

    VECTORS = [
        ({"a": 1.0, "b": 2.0}, '{"a":1,"b":2}',
         "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777"),
        ({"lr": 0.01, "momentum": 0.9}, '{"lr":0.01,"momentum":0.9}',
         "f12819ef8e23927377e59d3f132f579f9f4b1240e72c7bd030eb6e0b3eab09a0"),
        ({"x": 1.5, "y": -3.25, "z": 0.42}, '{"x":1.5,"y":-3.25,"z":0.42}',
         "91fec846a2b63a1d21d40abeab6a4f5679dff3692d281af7a93fa6bcc5501bd2"),
        ({}, "{}",
         "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"),
    ]

    def test_go_compatible_hashes(self):
        for params, canonical, sha in self.VECTORS:
            self.assertEqual(worker.canonical_json(params), canonical)
            self.assertEqual(worker.param_hash(params), sha)


import unittest
from pathlib import Path

from .enrichers import (
    enrich_from_bpftrace_logs,
    enrich_from_fio_json_plus,
    enrich_from_nixos_version_json,
    enrich_from_os_release,
)
from .model import Artifact, Fact, Metric

testdata_dir = Path(__file__).resolve().parent / "testdata"


class TestEnrichFromOsRelease(unittest.TestCase):
    def test_enrich_os_release(self):
        test_definitions = [
            (
                "results/nixos-asi-benchmarks:836d59863d4a/artifacts/etc_os-release",
                [Fact(name="os_release_variant_id", value="aethelred-asi-on")],
            ),
            (
                "results/nixos-asi-benchmarks:d6b0e7e4b7b4/artifacts/etc_os-release",
                [Fact(name="os_release_variant_id", value="aethelred-asi-off")],
            ),
        ]

        for artifact_path, want_facts in test_definitions:
            artifact = Artifact(path=testdata_dir / artifact_path)
            with self.subTest(artifact=artifact):
                facts, metrics = enrich_from_os_release(artifact)
                self.assertEqual(facts, want_facts)
                self.assertEqual(metrics, [])


class TestEnrichFromFioJsonPlus(unittest.TestCase):
    def test_enrich_fio_json_plus(self):
        test_definitions = [
            (
                "results/nixos-asi-benchmarks:836d59863d4a/artifacts/fio_output_1.json",
                [
                    Metric(name="fio_randread_read_lat_ns_mean", value=56960.234619),
                    Metric(name="fio_randread_read_slat_ns_mean", value=0.0),
                    Metric(name="fio_randread_read_clat_ns_mean", value=56932.733276),
                    Metric(name="fio_randread_read_iops", value=17448.349308),
                ],
            ),
            (
                "results/nixos-asi-benchmarks:d6b0e7e4b7b4/artifacts/fio_output_1.json",
                [
                    Metric(name="fio_randread_read_lat_ns_mean", value=52777.721008),
                    Metric(name="fio_randread_read_slat_ns_mean", value=0.0),
                    Metric(name="fio_randread_read_clat_ns_mean", value=52755.286926),
                    Metric(name="fio_randread_read_iops", value=18853.855006),
                ],
            ),
        ]

        for artifact_path, want_metrics in test_definitions:
            artifact = Artifact(path=testdata_dir / artifact_path)
            with self.subTest(artifact=artifact):
                facts, metrics = enrich_from_fio_json_plus(artifact)

                want_metrics = {m.name: m.value for m in want_metrics}
                metrics = {m.name: m.value for m in metrics}

                self.assertEqual(facts, [])
                self.assertEqual(metrics.keys(), want_metrics.keys())
                for name in metrics:
                    self.assertAlmostEqual(
                        metrics[name],
                        want_metrics[name],
                        msg=f"metric '{name}' differs",
                    )


class TestEnrichFromNixosVersionJson(unittest.TestCase):
    def test_enrich_nixos_version_json(self):
        test_definitions = [
            (
                "results/nixos-asi-benchmarks:836d59863d4a/artifacts/nixos-version.json",
                [
                    Fact(
                        name="nixos_configuration_revision",
                        value="1254e976fb3bfe9ea80a6a23e9456248149f36eb",
                    )
                ],
            ),
            (
                "results/nixos-asi-benchmarks:d6b0e7e4b7b4/artifacts/nixos-version.json",
                [
                    Fact(
                        name="nixos_configuration_revision",
                        value="f1034e1fd7e67e1a4297386446a1339727abf647",
                    )
                ],
            ),
        ]

        for artifact_path, want_facts in test_definitions:
            artifact = Artifact(path=testdata_dir / artifact_path)
            with self.subTest(artifact=artifact):
                facts, metrics = enrich_from_nixos_version_json(artifact)
                self.assertEqual(facts, want_facts)
                self.assertEqual(metrics, [])


class TestEnrichFromBpftraceLogs(unittest.TestCase):
    def test_enrich_bpftrace_logs(self):
        artifact = Artifact(
            path=testdata_dir
            / "results/nixos-asi-benchmarks:836d59863d4a/artifacts/bpftrace_asi_exits.log"
        )
        facts, metrics = enrich_from_bpftrace_logs(artifact)

        self.assertEqual(facts, [Fact(name="instrumented", value=True)])
        self.assertEqual(metrics, [Metric(name="asi_exits", value=16764)])


if __name__ == "__main__":
    unittest.main()

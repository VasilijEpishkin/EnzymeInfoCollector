"""Offline CLI integration tests: failure does not publish partial output."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from enzymeinfocollector.reference_cli import main
from enzymeinfocollector.sources import DataSourceError


class ReferenceCLITests(unittest.TestCase):
    def test_atomic_json_export(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "integrated.json"
            with patch("enzymeinfocollector.reference_cli.collect_live_reference",
                       return_value={"schema_version": "2.0-reference-preview", "proteins": []}) as collect:
                status = main(["--accession", "P00330", "--cache-dir", directory,
                               "--output", str(output)])
            self.assertEqual(status, 0)
            self.assertEqual(json.loads(output.read_text())["proteins"], [])
            collect.assert_called_once()

    def test_source_failure_preserves_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "integrated.json"
            output.write_text("existing result", encoding="utf-8")
            with patch("enzymeinfocollector.reference_cli.collect_live_reference",
                       side_effect=DataSourceError("simulated upstream error")):
                status = main(["--accession", "P00330", "--cache-dir", directory,
                               "--output", str(output)])
            self.assertEqual(status, 1)
            self.assertEqual(output.read_text(encoding="utf-8"), "existing result")


if __name__ == "__main__":
    unittest.main()

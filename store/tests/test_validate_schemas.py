"""Unit tests for schema validation and PII auditing."""

import json
import tempfile
import unittest
from pathlib import Path
import sys

# Add scripts directory to path
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import importlib
validate_schemas = importlib.import_module("validate-schemas")


class TestSchemaValidation(unittest.TestCase):
    def test_production_schemas_pass(self):
        """All production SQL and Arrow schemas in schemas/ must pass validation and PII audit."""
        result = validate_schemas.main()
        self.assertEqual(result, 0)

    def test_pii_audit_detection(self):
        """PII audit detects forbidden PII tokens."""
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = Path(tmpdir) / "test_pii.sql"
            test_file.write_text("CREATE TABLE test (cpf VARCHAR, card_number VARCHAR, email VARCHAR);", encoding="utf-8")
            violations = validate_schemas.audit_pii(test_file)
            self.assertTrue(len(violations) >= 3)
            self.assertTrue(any("cpf" in v for v in violations))
            self.assertTrue(any("card_number" in v for v in violations))
            self.assertTrue(any("email" in v for v in violations))

    def test_arrow_validation_guards(self):
        """Arrow schema validation catches missing fields and non-dict format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            invalid_json_file = Path(tmpdir) / "corrupt.json"
            invalid_json_file.write_text("not json", encoding="utf-8")
            errs = validate_schemas.validate_arrow_schema(invalid_json_file, "orders_raw_events")
            self.assertTrue(any("Invalid JSON" in e for e in errs))

            non_dict_file = Path(tmpdir) / "array.json"
            non_dict_file.write_text("[]", encoding="utf-8")
            errs2 = validate_schemas.validate_arrow_schema(non_dict_file, "orders_raw_events")
            self.assertTrue(any("must be a JSON object" in e for e in errs2))

            missing_field_file = Path(tmpdir) / "missing.json"
            missing_field_file.write_text(json.dumps({"fields": [{"name": "order_id"}]}), encoding="utf-8")
            errs3 = validate_schemas.validate_arrow_schema(missing_field_file, "orders_raw_events")
            self.assertTrue(any("missing required field 'payload_id'" in e for e in errs3))


if __name__ == "__main__":
    unittest.main()


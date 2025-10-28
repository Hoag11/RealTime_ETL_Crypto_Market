import os
import sys
import unittest
from unittest.mock import MagicMock


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from scripts.helpers.minio_utils import (
    bucket_has_objects,
    ensure_bucket,
    upload_json_bytes,
)


class TestMinioUtils(unittest.TestCase):
    def test_ensure_bucket_creates_when_missing(self):
        client = MagicMock()
        client.bucket_exists.return_value = False

        ensure_bucket(client, "test-bucket")

        client.make_bucket.assert_called_once_with("test-bucket")

    def test_ensure_bucket_skips_when_exists(self):
        client = MagicMock()
        client.bucket_exists.return_value = True

        ensure_bucket(client, "existing-bucket")

        client.make_bucket.assert_not_called()

    def test_upload_json_bytes_uses_bytes_stream(self):
        client = MagicMock()

        upload_json_bytes(client, "bucket", "object.json", "{\"key\": \"value\"}")

        client.put_object.assert_called_once()
        args, kwargs = client.put_object.call_args
        self.assertEqual(args[0], "bucket")
        self.assertEqual(args[1], "object.json")
        data_stream = kwargs["data"]
        self.assertEqual(data_stream.getvalue(), b'{"key": "value"}')
        self.assertEqual(kwargs["length"], len(b'{"key": "value"}'))
        self.assertEqual(kwargs["content_type"], "application/json")

    def test_bucket_has_objects_handles_missing_bucket(self):
        client = MagicMock()
        client.bucket_exists.return_value = False

        self.assertFalse(bucket_has_objects(client, "missing"))
        client.list_objects.assert_not_called()

    def test_bucket_has_objects_detects_object(self):
        client = MagicMock()
        client.bucket_exists.return_value = True
        client.list_objects.return_value = iter([object()])

        self.assertTrue(bucket_has_objects(client, "bucket", prefix="foo/"))

    def test_bucket_has_objects_detects_empty(self):
        client = MagicMock()
        client.bucket_exists.return_value = True
        client.list_objects.return_value = iter([])

        self.assertFalse(bucket_has_objects(client, "bucket"))


if __name__ == "__main__":
    unittest.main()

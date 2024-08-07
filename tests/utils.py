import unittest
import sqrl.utils as utils

class TestExtractFilename(unittest.TestCase):
    def test_extract_filename(self):
        self.assertEqual(utils.extract_filename("hello/world.txt"), "world")
        self.assertEqual(utils.extract_filename("world"), "world")

class TestDetectType(unittest.TestCase):
    def test_detect_type_csv(self):
        self.assertEqual(utils.detect_type_csv(1), "integer")
        self.assertEqual(utils.detect_type_csv(1.0), "real")
        self.assertEqual(utils.detect_type_csv(1.2), "real")
        self.assertEqual(utils.detect_type_csv("1.2"), "real")
        self.assertEqual(utils.detect_type_csv("1"), "integer")
        self.assertEqual(utils.detect_type_csv(b"\xDE\xEA\xBE\xEF"), "blob")
        self.assertEqual(utils.detect_type_csv(b"hello world"), "blob")
        self.assertEqual(utils.detect_type_csv("hello world"), "text")

    def test_detect_type_json(self):
        self.assertEqual(utils.detect_type_json(1), "integer")
        self.assertEqual(utils.detect_type_json(1.0), "real")
        self.assertEqual(utils.detect_type_json(1.2), "real")
        self.assertEqual(utils.detect_type_json("1.2"), "text")
        self.assertEqual(utils.detect_type_json("1"), "text")
        self.assertEqual(utils.detect_type_json(b"\xDE\xEA\xBE\xEF"), "blob")
        self.assertEqual(utils.detect_type_json(b"hello world"), "blob")
        self.assertEqual(utils.detect_type_json("hello world"), "text")


if __name__ == '__main__':
    unittest.main()

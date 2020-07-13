import unittest

from luigi_report_utils import inpt


class TestInpt(unittest.TestCase):
    def test_from_str(self):
        STR_IN = "This is a test"
        with inpt.from_str(STR_IN).open() as f:
            self.assertEqual(f.read(), STR_IN)


if __name__ == "__main__":
    unittest.main()

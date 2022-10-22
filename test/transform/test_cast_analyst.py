import unittest

from graph_cast.util.transform import cast_ibes_analyst


class TestCastAnalyst(unittest.TestCase):

    examples = ["ADKINS/NARRA", "/ZHANG/LI/YA", "/ZHANG/LI", "ARFSTROM      J"]
    result = [
        ("ADKINS", "N"),
        ("ZHANG", "L"),
        ("ZHANG", "L"),
        ("ARFSTROM", "J"),
    ]

    def test_a(self):
        r = [cast_ibes_analyst(e) for e in self.examples]
        print(r)
        for x, y in zip(r, self.result):
            self.assertEqual(x, y)


if __name__ == "__main__":
    unittest.main()

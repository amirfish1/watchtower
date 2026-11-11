import unittest

from watchtower import workers


class MinorBlockContractTest(unittest.TestCase):
    def test_contract_names_gate_flags(self):
        c = workers.MINOR_BLOCK_CONTRACT
        for flag in ("--class code-change-revertable", "--weight minor",
                     "--undo", "--attest", "--commit"):
            self.assertIn(flag, c)

    def test_contract_in_both_worker_prompts(self):
        src = open(workers.__file__).read()
        self.assertEqual(src.count("+ MINOR_BLOCK_CONTRACT +"), 2)


if __name__ == "__main__":
    unittest.main()

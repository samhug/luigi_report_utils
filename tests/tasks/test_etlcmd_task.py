import luigi
import unittest
import tempfile
import os

from luigi_report_utils import inpt
from luigi_report_utils.tasks import EtlcmdTask


class TestEtlcmdTask(unittest.TestCase):
    def test_etlcmd_task(self):

        # Create a temporary directory and save the test input file to it
        temp_dir = tempfile.TemporaryDirectory()

        input_file = os.path.join(temp_dir.name, "input.jsonl")
        with open(input_file, "w") as f:
            f.writelines(
                [
                    '{ "A":1, "B":10, "C":100 }\n',
                    '{ "A":2, "B":20, "C":200 }\n',
                    '{ "A":3, "B":30, "C":300 }\n',
                ]
            )

        output_file = os.path.join(temp_dir.name, "output.jsonl")

        config_file = os.path.join(temp_dir.name, "config.hcl")
        with open(config_file, "w") as f:
            f.write(
                """
process "Test Process" {
    input "jsonl" {
        path = "%s"
    }
    output "jsonl" {
        path = "%s"
    }
}
"""
                % (input_file, output_file)
            )

        success = luigi.build(
            [EtlcmdTask(config_file=config_file, output_path=output_file)],
            workers=1,
            local_scheduler=True,
        )
        self.assertTrue(success)

        with open(input_file, "r") as f_in:
            with open(output_file, "r") as f_out:
                self.assertEqual(f_in.read(), f_out.read())


if __name__ == "__main__":
    unittest.main()

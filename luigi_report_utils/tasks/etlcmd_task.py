import os
import tempfile
import luigi

from luigi.contrib.external_program import ExternalProgramTask


class EtlcmdTask(ExternalProgramTask):
    task_namespace = "etlcmd"
    config_file = luigi.Parameter()
    output_path = luigi.Parameter()

    def program_args(self):
        return ["etlcmd", "--config", self.config_file]

    def run(self):
        self.output().makedirs()
        super(EtlcmdTask, self).run()

    def output(self):
        return luigi.LocalTarget(path=self.output_path)

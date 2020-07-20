import os
import tempfile
import luigi

from . import EtlcmdTask


class UdtExportTask(luigi.WrapperTask):

    udt_host = luigi.Parameter()
    udt_username = luigi.Parameter()
    udt_password = luigi.Parameter()
    udt_bin = luigi.Parameter()
    udt_home = luigi.Parameter()
    udt_acct = luigi.Parameter()

    process = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        config = """
unidata {
	host = "%s"

	username = "%s"
	password = "%s"

	udtbin  = "%s"
	udthome = "%s"
	udtacct = "%s"
}
%s
""" % (
            self.udt_host,
            self.udt_username,
            self.udt_password,
            self.udt_bin,
            self.udt_home,
            self.udt_acct,
            self.process,
        )

        # Create a temporary directory and save the etlcmd config to it
        self.temp_dir = tempfile.TemporaryDirectory(prefix="luigi_etlcmd_task")
        config_file = os.path.join(self.temp_dir.name, "config.hcl")
        with open(config_file, "w") as f:
            f.write(config)

        return EtlcmdTask(config_file=config_file, output_path=self.output_path)

from filebeat import TestCase
import os
import socket
import shutil

"""
Tests for the multiline log messages
"""


class Test(TestCase):
    def test_java_elasticsearch_log(self):
        """
        Test that multi lines for java logs works.
        It checks that all lines which do not start with [ are append to the last line starting with [
        """
        self.render_config_template(
            path=os.path.abspath(self.working_dir) + "/log/*",
            multiline=True,
            pattern="^\[",
            negate="true",
            match="after"
        )

        os.mkdir(self.working_dir + "/log/")
        shutil.copy2("../files/logs/elasticsearch-multiline-log.log", os.path.abspath(self.working_dir) + "/log/elasticsearch-multiline-log.log")

        proc = self.start_filebeat()

        # wait for the "Skipping file" log message
        self.wait_until(
            lambda: self.output_has(lines=20),
            max_timeout=10)

        proc.kill_and_wait()

        output = self.read_output()

        # Check that output file has the same number of lines as the log file
        assert 20 == len(output)

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.
import os


class FileSystemReaderWriter(object):
    """
    From what I can tell, the FileSystemReaderWriter class munges two concerns:
    (1) Making sure a file or a directory exists and creating that file or directory if it
        doesn't already exist
    (2) Reading in a file if the path exists or returning an empty string if it doesn't.
    """

    def __init__(self, path):
        from .utils import expand_path
        assert path is not None
        self.path = expand_path(path)

    def ensure_path_exists(self):
        """Create a directory if `self.path` doesn't exist"""
        self._ensure_path_exists(self.path)

    def ensure_file_exists(self):
        """Create an empty file if `self.path` doesn't exist"""
        self._ensure_path_exists(os.path.dirname(self.path))
        if not os.path.exists(self.path):
            open(self.path, 'w').close()

    def read_lines(self):
        """Read the contents of `self.path` if it is a file. Otherwise return an empty string
        """
        if os.path.isfile(self.path):
            with open(self.path, "r") as f:
                return f.readlines()
        else:
            return ""

    def overwrite_with_line(self, line):
        """Create a file if it doesn't exist. Otherwise truncate the file and write `lines` to it

        `w+':  Open for reading and writing.  The file is created if it does not
               exist, otherwise it is truncated.  The stream is positioned at
               the beginning of the file.
        """

        with open(self.path, "w+") as f:
            f.writelines(line)

    def _ensure_path_exists(self, path):
        """Make a directory and all intermediate directories. Raise an OSError
        if the creation was unsuccessful"""
        try:
            os.makedirs(path)
        except OSError:
            if not os.path.isdir(path):
                raise

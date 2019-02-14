import logging

from .utils import join_paths, get_instance_id
from .filesystemreaderwriter import FileSystemReaderWriter


class MagicsFileHandler(logging.FileHandler):
    """The default logging handler used by the magics; this behavior can be overridden by modifying the config file"""
    def __init__(self, **kwargs):
        # Simply invokes the behavior of the superclass, but sets the filename keyword argument if it's not already set.
        if 'filename' in kwargs:
            super(MagicsFileHandler, self).__init__(**kwargs)
        else:
            # I guess you're popping this off because logging.FileHandler doesn't expect it?
            magics_home_path = kwargs.pop(u"home_path")
            logs_folder_name = "logs"
            log_file_name = "log_{}.log".format(get_instance_id())
            directory = FileSystemReaderWriter(join_paths(magics_home_path, logs_folder_name))
            directory.ensure_path_exists()
            super(MagicsFileHandler, self).__init__(
                filename=join_paths(directory.path, log_file_name), **kwargs)

# Suggest simplifying this __init__ function to be something more like this:

    # def __init__(self, **kwargs):
    #     if 'filename' not in kwargs:
    #         # If we're not explicitly given a log filename then we construct it from the
    #         # configuration
    #         magics_home_path = kwargs.pop(u"home_path")
    #         logs_dir = os.path.join(magics_home_path, 'logs')
    #         # Ensure that our directory exists.
    #         os.makedirs(logs_dir)

    #         # Format the log file name and its full path
    #         log_file_name = "log_{}.log".format(get_instance_id())
    #         log_file = os.path.join(logs_dir, log_file_name)
    #         kwargs['filename'] = log_file

    #     super(MagicsFileHandler, self).__init__(**kwargs)



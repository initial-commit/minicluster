def _bootstrap():
    import warnings
    w_ctx = warnings.catch_warnings(record=True)
    w_list = w_ctx.__enter__()
    warnings.simplefilter("always")
    import sys
    import os
    sys.pycache_prefix = os.getcwd() + "/tmp/__pycache__"
    import time
    now = time.time()
    import collections
    import logging
    import argparse
    from dateutil.relativedelta import relativedelta

    ######################################################
    # data structure
    # the goal of this struct is to store data that is constant
    # during the run of a command
    minicluster = collections.namedtuple("MINICLUSTER", [
        "DIR_R", # root directory of the project
        "COMMAND", # the command script executed
        "TIME_START", # time when command started in nanoseconds
        "CWD_START", # workind directory at bootstrap
        "ARGPARSE", # argument parser
        "ARGS", # named args
        "POS_ARGS", # positional args
        "bootstrap_finished", # function used to signal, must be called by each command
	"w_ctx",
	"w_list",
    ])

    ######################################################
    # paths and other simple values
    root = pf"{__file__}".resolve().parent.parent.parent
    src = root / "src" / "python"
    bin = root / "bin" / "commands"

    sys.path.append(str(src))
    cmd=p"$XONSH_SOURCE".resolve().name

    cwd = os.getcwd()

    if str(bin) not in $PATH:
        $PATH.append(str(bin))

    ######################################################
    # arguments
    args = {}
    pos_args = []
    class CommandArgumentParser(argparse.ArgumentParser):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.add_argument('--log', default='INFO')
        def parse_known_args(self):
            return super().parse_known_args(sys.argv)

    argparser = CommandArgumentParser(add_help=False)
    loglevel = argparser.parse_known_args()[0].log
    argparser = CommandArgumentParser()

    ######################################################
    numeric_level = getattr(logging, loglevel.upper(), None)
    logging.basicConfig(level=numeric_level, format="[%(asctime)s] [%(levelname)-8s] [%(name)s LN %(lineno)3d FN %(funcName)10s] - %(message)s")

    #TODO: add time since start, make it human readable as a duration
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        if record.name == '__main__':
            record.name = cmd
        record.custom_attribute = "my-attr"
        return record

    logging.setLogRecordFactory(record_factory)

    ######################################################
    #TODO: set up xonsh hooks
    @events.on_exit
    def on_exit():
        now = time.time()
        start_s = MINICLUSTER.TIME_START
        diff_ns = now - MINICLUSTER.TIME_START
        diff = relativedelta(seconds=now-start_s)
        attrs = ['years', 'months', 'days', 'hours', 'minutes', 'seconds']
        human_readable = lambda delta: ['%d %s' % (getattr(delta, attr), attr if getattr(delta, attr) > 1 else attr[:-1]) for attr in attrs if getattr(delta, attr)]
        logger = logging.getLogger(__name__)
        ms = int(diff_ns * 1_000_000 % 1_000_000)
        d = human_readable(diff)
        d.append(f"{ms} microseconds")
        logger.info(f"command took {d}")


    ######################################################
    #TODO: install xonsh hooks https://xon.sh/events.html


    ######################################################
    # MINICLUSTER global
    MINICLUSTER = minicluster(
        root,
        cmd,
        now,
        cwd,
        argparser,
        args,
        pos_args,
        None,
	w_ctx,
	w_list,
    )
    return MINICLUSTER


MINICLUSTER = _bootstrap()
######################################################
#create hook for bootstrap finished
def bootstrap_finished(MINICLUSTER):
    import logging
    t = MINICLUSTER.ARGPARSE.parse_known_args()
    MINICLUSTER = MINICLUSTER._replace(ARGS=t[0], POS_ARGS=t[1])

    #@events.on_import_post_exec_module
    def on_import_post_exec_module(module):
        logger = logging.getLogger(__name__)
        logger.info(f"{module=}")

    #@events.on_import_post_create_module
    def on_import_post_create_module(module, spec):
        logger = logging.getLogger(__name__)
        logger.info(f"{module=} {spec=}")

    #@events.on_import_pre_find_spec
    def on_import_pre_find_spec(fullname, path, target):
        logger = logging.getLogger(__name__)
        logger.info(f"{fullname=} {path=} {target=}")

    @events.on_post_cmdloop
    def on_post_cmdloop():
        logger = logging.getLogger(__name__)
        logger.info(f"event triggered")

    @events.on_postcommand
    def on_postcommand(**kwargs):
        logger = logging.getLogger(__name__)
        logger.info(f"{kwargs=}")

    @events.on_precommand
    def on_precommand(**kwargs):
        logger = logging.getLogger(__name__)
        logger.info(f"{kwargs=}")

    @events.on_transform_command
    def on_transform_command(**kwargs):
        logger = logging.getLogger(__name__)
        logger.info(f"{kwargs=}")

    logger = logging.getLogger(__name__)
    for w in MINICLUSTER.w_list:
    	logger.info(f"BOOTSTRAP WARNING: {w=} {str(w)}")
    logger.info(f"warnings created during bootstrap: {len(MINICLUSTER.w_list)}")
    MINICLUSTER.w_ctx.__exit__(None, None, None)
    assert len(MINICLUSTER.w_list) == 0, "Some warnings shown during bootstrap"
    return MINICLUSTER
MINICLUSTER = MINICLUSTER._replace(bootstrap_finished = bootstrap_finished)

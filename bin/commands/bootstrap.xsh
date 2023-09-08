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
    import signal
    import pathlib
    from xonsh.tools import unthreadable
    #import faulthandler

    ######################################################
    # data structure
    # the goal of this struct is to store data that is constant
    # during the run of a command
    minicluster = collections.namedtuple("MINICLUSTER", [
        "DIR_R", # root directory of the this project
        "DIR_M", # the directory of the module (can be DIR_R or module from layouts/ or external modules)
        "COMMAND", # the command script executed
        "TIME_START", # time when command started in nanoseconds
        "CWD_START", # workind directory at bootstrap
        "ARGPARSE", # argument parser
        "ARGS", # named args
        "POS_ARGS", # positional args
        "MODULES", # minicluster-type modules / layouts / plugins
        "bootstrap_finished", # function used to signal, must be called by each command
        "w_ctx",
        "w_list",
        "signal_handler",
    ])

    ######################################################
    # paths and other simple values
    root = pf"{__file__}".resolve().parent.parent.parent
    src = root / "src" / "python"
    bin = root / "bin" / "commands"
    dir_m = None

    sys.path.append(str(src))
    cmd=p"$XONSH_SOURCE".resolve().name
    #start_dir=p"$XONSH_SOURCE".resolve().parent

    cwd = os.getcwd()

    if str(bin) not in $PATH:
        $PATH.append(str(bin))

    modules = []
    for p in map(pathlib.Path, $PATH):
        b = p / 'bootstrap.xsh'
        if not b.exists():
            continue
        src = p.parent.parent / 'src' / 'python'
        module_root = p.parent.parent
        if (p / cmd).exists():
            dir_m = module_root

        modules.append({'root': str(module_root), 'bin': str(p)})
        if src.exists() and str(src.resolve()) not in sys.path:
            sys.path.append(str(src))

    assert dir_m is not None

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
    logging.basicConfig(
        level=numeric_level,
        format="[%(asctime)s] [%(levelname)-8s] [%(name)s LN %(lineno)3d FN %(funcName)10s] - %(message)s",
        #stream=sys.stdout
    )

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
    logger = logging.getLogger(__name__)
    @unthreadable
    def _source_command(args, stdin):
        for a in args:
            for m in modules:
                b = m['bin']
                s = pf"{b}/{a}"
                logger.info(f"sourcing command {s}")
                source @(s)
                return True
        raise Exception(f"Could not find command in modules {args=} {modules=}")

    aliases['source_command'] = _source_command

    ######################################################
    #TODO: install xonsh hooks https://xon.sh/events.html

    ######################################################
    # signal handling
    class SignalHandler(object):
        def __init__(self, minicluster):
            #faulthandler.enable()
            self.MINICLUSTER = minicluster
            self.logger = logging.getLogger(__name__)
            catchable_sigs = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP}
            catchable_sigs.remove(signal.SIGCHLD) # TODO: check this out, more logging / monitoring
            for sig in catchable_sigs:
                signal.signal(sig, self.handler)  # Substitute handler of choice for `print`
            #signal.signal(signal.SIGINT, self.handler)
            #signal.signal(signal.SIGTERM, self.handler)
            #signal.signal(signal.SIGUSR1, self.handler)
            #signal.signal(signal.SIGUSR2, self.handler)
            #signal.signal(signal.SIGABRT, self.handler)
            #signal.signal(signal.SIGPIPE, self.handler)
            self.logger.info(f"signal handlers installed")
        def handler(self, sig, frame):
            self.logger.info(f"received {sig=} at {frame=}")
            if sig == signal.SIGINT:
                self.logger.info(f"CTRL+C")
                # TODO: keep track of how many times we got it at a short interval
                # if more than 2, and the same frame, print frame and exit
                sys.exit(0)

    ######################################################
    # MINICLUSTER global
    MINICLUSTER = minicluster(
        root,
        dir_m,
        cmd,
        now,
        cwd,
        argparser,
        args,
        pos_args,
        modules,
        None,
        w_ctx,
        w_list,
        SignalHandler,
    )
    return MINICLUSTER


MINICLUSTER = _bootstrap()

######################################################
#TODO: set up xonsh hooks
@events.on_exit
def on_exit():
	from dateutil.relativedelta import relativedelta
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


def early_exit(code):
	on_exit()
	sys.exit(code)

######################################################
#create hook for bootstrap finished
def bootstrap_finished(MINICLUSTER):
    import logging
    t = MINICLUSTER.ARGPARSE.parse_known_args()
    MINICLUSTER = MINICLUSTER._replace(ARGS=t[0], POS_ARGS=t[1])
    MINICLUSTER = MINICLUSTER._replace(signal_handler = MINICLUSTER.signal_handler(MINICLUSTER))

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
    logger.info(f"{MINICLUSTER.DIR_R=}")
    logger.info(f"{MINICLUSTER.DIR_M=}")
    logger.info(f"{MINICLUSTER.COMMAND=}")
    logger.info(f"{MINICLUSTER.CWD_START=}")
    MINICLUSTER.w_ctx.__exit__(None, None, None)
    assert len(MINICLUSTER.w_list) == 0, "Some warnings shown during bootstrap"
    return MINICLUSTER
MINICLUSTER = MINICLUSTER._replace(bootstrap_finished = bootstrap_finished)

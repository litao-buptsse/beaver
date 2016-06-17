#!/usr/bin/python
#
# Execute commands given by cli arguments, and make sure that:
# When this python process is killed by a signal other than -9,
# all its's child and child's child process is killed.

import signal
import time
import sys
import os
import subprocess
from datetime import datetime

global child_pid

def printCmdRunnerInfo(msg):
	print >> sys.stderr, "%s [%s] %s" % (RUNNER_FLAG, datetime.now().strftime('%Y%m%d %H:%M:%S'), msg)
RUNNER_FLAG='CMD_RUNNER_INFO'
def termHandler(signum, frame):
	printCmdRunnerInfo("killing process group: %s using SIGTERM" % str(child_pid))
	os.killpg(child_pid, signal.SIGTERM)

pid = os.fork()
if pid > 0:
	# I'm the parent
	signal.signal(signal.SIGTERM, termHandler)

	child_pid = pid
	while True:
		try:
			_, exit_status = os.waitpid(child_pid, 0)

			if os.WIFEXITED(exit_status):
				exit_code = os.WEXITSTATUS(exit_status)
			elif os.WIFSIGNALED(exit_status):
				exit_code = os.WTERMSIG(exit_status)
			else:
				exit_code = exit_status
			printCmdRunnerInfo("EXITED childPid:%d  exitCode: %d" % (child_pid, exit_code))
			sys.exit(exit_code)
		except OSError, e:
			continue


elif pid == 0:
	# I'm the child
	# Set process group id to my pid, so we created a new process group,
	# and I become the leader of this process group.
	os.setpgid(os.getpid(), os.getpid())

	cmd = " ".join(sys.argv[1:])
	printCmdRunnerInfo("START %s" % cmd)
	os.execv("/bin/bash", ["/bin/bash", "-c", cmd])
else:
	print >> sys.stderr, "runner.py fork error"

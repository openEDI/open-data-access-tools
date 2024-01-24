import subprocess
import sys

args = sys.argv
subprocess.run(['azcopy', '--version'])

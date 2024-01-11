import subprocess
import sys

args = sys.argv
print(args[1])
subprocess.run(['azcopy', '--version'])

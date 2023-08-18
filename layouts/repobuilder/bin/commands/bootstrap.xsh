d=p"$XONSH_SOURCE".resolve().parent.resolve()
import pathlib
for p in map(pathlib.Path, $PATH):
	if p.resolve() == d:
		continue
	b = p / 'bootstrap.xsh'
	if not b.exists():
		continue
	source @(f"{b}")

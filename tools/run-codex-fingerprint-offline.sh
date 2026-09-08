#!/bin/sh
# Run only a Linux test binary in a fresh network namespace, never via WSL interop.
set -eu
if [ "${1:-}" != --inside ]; then
  test "$#" -eq 2 || { echo 'usage: run-codex-fingerprint-offline.sh BINARY TEST_PATTERN' >&2; exit 2; }
  # Include namespace setup and package init in the 60-second wall-clock bound.
  exec timeout --signal=TERM --kill-after=2s 58s unshare --net sh "$0" --inside "$1" "$2"
fi
shift
binary=$1
pattern=$2

ip link set lo up
test "$(ip -o link show | wc -l)" -eq 1
test -z "$(ip route show)"
test -z "$(ip -6 route show)"
python3 - "$binary" <<'PY'
import errno
import socket
import sys
with open(sys.argv[1], 'rb') as f:
    if f.read(4) != b'\x7fELF':
        raise SystemExit('Refusing a non-Linux binary: WSL interop would bypass isolation')
for family, address in [(socket.AF_INET, ('198.51.100.1', 443)),
                        (socket.AF_INET6, ('2001:db8::1', 443))]:
    with socket.socket(family, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.connect(address)
        except OSError as e:
            if e.errno != errno.ENETUNREACH:
                raise
        else:
            raise SystemExit('Network isolation self-check failed')
print('Isolation OK: Linux ELF, loopback only, IPv4/IPv6 egress blocked', flush=True)
PY

exec setpriv --bounding-set=-all --no-new-privs env -i PATH=/usr/bin:/bin HOME=/tmp GIN_MODE=test \
  "$binary" -test.run "$pattern" -test.timeout=55s -test.v

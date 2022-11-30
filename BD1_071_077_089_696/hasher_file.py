#!/usr/bin/env python3
import sys
import random
no = int(sys.argv[1])

for line in sys.stdin:
    # line = line.strip()
    # word, count = line.rsplit(",", 1)
    print(random.randrange(0, no))
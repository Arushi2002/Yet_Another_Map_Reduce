#!/usr/bin/env python3
import sys

current_word = None
current_count = 0
word = None

for line in sys.stdin:
    line = line.strip()

    word, count = line.split(',')

    try:
        count = int(count)
    except ValueError:
        continue

    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print(current_word, current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print (current_word, current_count)

#!/usr/bin/env python3
# import sys

# query_word = sys.argv[1]
# total_count = 0
# for line in sys.stdin:
#     line = line.strip()
#     word, count = line.rsplit(",", 1)
#     count = int(count)
#     #if query_word == word:
#     total_count += 1
# print(total_count)
#!/usr/bin/env python3
"""Generic combiner + reducer: input 'key<TAB>n1,n2,...' sorted by key; output the
element-wise sum per key. Used as the COMBINER (map side) and the REDUCER."""
import sys

def fmt(x):
    r = round(x, 2)
    return str(int(r)) if r == int(r) else repr(r)

def main():
    cur, acc = None, None
    for line in sys.stdin:
        key, _, val = line.rstrip("\n").partition("\t")
        nums = [float(v) for v in val.split(",")]
        if key == cur:
            for i, v in enumerate(nums):
                acc[i] += v
        else:
            if cur is not None:
                sys.stdout.write(cur + "\t" + ",".join(fmt(v) for v in acc) + "\n")
            cur, acc = key, nums
    if cur is not None:
        sys.stdout.write(cur + "\t" + ",".join(fmt(v) for v in acc) + "\n")

if __name__ == "__main__":
    main()

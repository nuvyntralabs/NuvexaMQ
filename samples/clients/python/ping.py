#!/usr/bin/env python3
"""Hello, including the cluster token field, then ping."""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from nuvexa import connect

def main():
    client = connect("python")
    client.ping()
    client.close()


if __name__ == "__main__":
    main()

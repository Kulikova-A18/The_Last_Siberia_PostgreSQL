import sys
from .cli import app
from .demo import demo_analysis

if __name__ == "__main__":
    if len(sys.argv) == 1:
        demo_analysis()
    else:
        app()

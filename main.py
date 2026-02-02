#!/usr/bin/env python
"""
NewsData.io – Pipeline Bronze

Ponto de entrada principal do projeto.

Uso:
    python main.py [opções]

Exemplos:
    python main.py
    python main.py --country br
    python main.py --category technology
    python main.py --query "bitcoin" --size 20
"""

import sys
from src.bronze.ingest import main

if __name__ == "__main__":
    sys.exit(main())

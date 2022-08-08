#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/5 22:51
# @Author  : Dominolu
# @File    : __init__.py.py
# @Software: PyCharm
from dynaconf import Dynaconf

from pathlib import Path
_BASE_DIR = Path(__file__).parent.parent

cfg = Dynaconf(
    envvar_prefix="ISLAND",
    settings_files=['settings.yaml', '.secrets.yaml'],
    base_dir=_BASE_DIR
)

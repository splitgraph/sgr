#!/bin/bash

git clone git://github.com/Kozea/Multicorn.git
cd Multicorn

git checkout 99ea772c4adf801d4178eb3854036d52bbce0aaa
# Apply unmerged patch from https://github.com/Kozea/Multicorn/pull/214 fixing build on PG11
# Not merged because of some issues with regression tests?
git cherry-pick e85bdec8009778302d632ad4cb0349858d16436e

PYTHON_OVERRIDE=python3.6 make install


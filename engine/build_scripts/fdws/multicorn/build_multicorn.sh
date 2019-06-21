#!/bin/bash

git clone git://github.com/Kozea/Multicorn.git
cd Multicorn

git checkout 99ea772c4adf801d4178eb3854036d52bbce0aaa

# Apply unmerged patch from https://github.com/Kozea/Multicorn/pull/214 fixing build on PG11
# Not merged because of some issues with regression tests?
git cherry-pick e85bdec8009778302d632ad4cb0349858d16436e

# Apply unmerged patch fixing https://github.com/Kozea/Multicorn/issues/136
# (Multicorn sometimes can initialize the Python interpreter rather than plpython,
# thus plpy doesn't get imported which breaks plpython functions (???)
git cherry-pick 485c5c6c0dae58b5c6dd093809d83f09fc56f8d4

PYTHON_OVERRIDE=python3 make install

#!/bin/sh
echo assuring ctools on master
(cd ctools; git checkout master; git pull)
git fetch --all
git checkout -b update-ctools || git checkout update-ctools
git merge origin/master
git merge master
git add ctools
git commit -m 'updated ctools'
git push origin update-ctools
git checkout master

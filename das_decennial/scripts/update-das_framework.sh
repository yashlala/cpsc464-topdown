#!/bin/sh
WHAT=das_framework
echo assuring $WHAT on master
(cd $WHAT; git checkout master; git pull)
git fetch --all
git checkout -b update-$WHAT || git checkout update-$WHAT
git merge origin/master
git merge master
git add $WHAT
git commit -m "updated $WHAT"
git push origin update-$WHAT
git checkout master

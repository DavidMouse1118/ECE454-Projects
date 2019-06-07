#!/bin/sh

FNAME=a1.tar.gz

tar -czf $FNAME *.java a1.thrift group.txt

echo
echo Your tarball file name is: $FNAME
echo

cat group.txt | grep bsimpson > /dev/null
if [ $? -eq 0 ]; then
    echo
    echo "YOU FORGOT TO EDIT THE group.txt FILE!!!"
    echo
else
    echo Your group members are: `cat group.txt`
    echo
    echo Good luck!
    echo
fi

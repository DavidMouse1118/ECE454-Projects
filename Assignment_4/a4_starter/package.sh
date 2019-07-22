#!/bin/sh

FNAME=a4.tar.gz

tar -czf $FNAME A4Application.java group.txt

cat group.txt | grep bsimpson > /dev/null
if [ $? -eq 0 ]; then
    echo
    echo ERROR: you forgot to edit the group.txt file !!!
    echo
    exit -1
else
    echo
    echo Your group members are: `cat group.txt`
    echo
fi

echo Your tarball file name is: $FNAME
echo 
echo It contains the following files:
echo 
tar -tf $FNAME

echo
echo Good luck!
echo

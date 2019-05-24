#!/bin/sh

FNAME=ece454a0.tar.gz

tar -czf $FNAME *.java group.txt

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

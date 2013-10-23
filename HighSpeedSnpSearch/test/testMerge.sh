set -e
mkfifo fifo1 fifo2
perl ../strainsTextToBinary 1 < strain1.txt > fifo1 &
perl ../strainsTextToBinary 2 < strain2.txt > fifo2 &
echo hello
../mergeStrains fifo1 1 fifo2 2 | ../dumpStrains 0
rm fifo1 fifo2

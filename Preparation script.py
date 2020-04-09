from os import chdir
import re

def readnumlines(file, num=2):
    f = iter(file)
    while True:
        lines = [None] * num
        for i in range(num):
            try:
                lines[i] = next(file)
            except StopIteration: # EOF or not enough lines available
                return
        yield lines

chdir(r'Apache Spark\Spark cheatsheets')

input_file = 'Spark book Chapters codes notepad.txt'
output_file = 'Spark book Chapters codes notepad.md'

file_in = open(input_file, 'r')
file_out = open(output_file, 'w+')

text1 = '# COMMAND ----------\n'

for line in file_in.readlines():
    if re.search('.py\n', line):
        line += text1
        file_out.write(line)
    else:
        file_out.write(line)

file_out.close()
file_in.close()

input_file = 'Spark book Chapters codes notepad.md'
output_file = 'Spark book Chapters codes notepad-final.md'
file_in = open(input_file, 'r')
file_out = open(output_file, 'w+')

text1 = '```\n'
text2 = '# COMMAND ----------\n'
text3 = '## Proper explanation\n'
text4 = '```python\n'

for line in file_in.readlines():
    if re.search(text2, line):
        line = text1 + text3 + text4
        file_out.write(line)
    else:
        file_out.write(line)

file_out.close()
file_in.close()

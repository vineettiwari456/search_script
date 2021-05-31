import re
import csv


FILE_PATH = "../res/index_synonyms.txt"

with open(FILE_PATH) as f:
    synonym_lines = f.readlines()
synonyms = {}

# storing lines in which phrase is present
for index, line in enumerate(synonym_lines):
    for phrase in [x.strip() for x in line.split(",")]:
        if phrase in synonyms:
            synonyms[phrase].append(index)
        else:
            synonyms[phrase] = [index]
print("Number of synonyms phrases: %d" % len(synonyms))

overlapping_synonyms = []
count = 1
# checking if phrase1 is present in phrase2
with open("overlapping_synonyms.csv", 'w', newline="") as file:
    writer = csv.writer(file)
    for phrase1 in synonyms.keys():
        if count % 1000 == 0:
            print(count)
        for phrase2 in synonyms.keys():
            # ignore if phrase1 and phrase2 are in same line or if phrase1 is longer than phrase2
            if synonyms[phrase1] == synonyms[phrase2] or len(phrase1) > len(phrase2):
                continue
            regex = re.compile(r"([^\w\.]|^)(" + re.escape(phrase1) + r")([^a-zA-Z_\+#]|$)", re.IGNORECASE)
            if regex.match(phrase2):
                writer.writerow([synonym_lines[index] for index in synonyms[phrase1] + synonyms[phrase2]])
                overlapping_synonyms.append((phrase1, synonyms[phrase1], phrase2, synonyms[phrase2]))
                break
        count += 1
for x in overlapping_synonyms:
    print(x)

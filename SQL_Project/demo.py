import json
with open("cs_papers_cleaned.json", mode="r", encoding="utf-8") as f:
    file = json.load(f)

subjects = []
for i in file:
    for j in i["academic_fields"]:
        if j not in subjects: subjects.append(j)
        
print(", ".join(subjects))
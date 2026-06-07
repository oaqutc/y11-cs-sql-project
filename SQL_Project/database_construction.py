import sqlite3, json

def create_table(con):
    cur = con.cursor()
    cur.execute("""
                   CREATE TABLE IF NOT EXISTS cs_papers(
                       paper_id INTEGER PRIMARY KEY
                       , title TEXT NOT NULL
                       , authors TEXT NOT NULL
                       , publication_date TEXT
                       , journal_name TEXT
                       , citation_count INTEGER NOT NULL
                       , academic_field TEXT NOT NULL
                       , pdf_link TEXT
                   ) STRICT
                """)
    
def insert_data(con, file):
    cur = con.cursor()
    i = 1
    data = []
    for paper in file:
        data.append((
            i
            , paper["title"]
            , paper["author"]
            , paper["publication_date"]
            , paper["journal_name"]
            , paper["citation_count"]
            , ", ".join(paper["academic_fields"])
            , paper["pdf_link"]
        ))
        i+=1
    cur.executemany("INSERT INTO cs_papers VALUES(?, ?, ?, ?, ?, ?, ?, ?)", data)
    con.commit()

def main():
    con = sqlite3.connect("cs_papers.db")
    with open("cs_papers_cleaned.json", mode = "r", encoding = "utf-8") as f:
        file = json.load(f)
    
    create_table(con)
    insert_data(con, file)
    
if __name__ == "__main__":
    main()
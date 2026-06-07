import sqlite3

def update_results(
    con: sqlite3.Connection, 
    filters: list = None, 
    page_num: int = 1, 
    order_type: str = "citation_count", 
    order_desc: bool = True
):
    cur = con.cursor()
    page_size = 50
    offset = (page_num - 1) * page_size
    direction = "DESC" if order_desc else "ASC"
    
    if not filters:
        count = cur.execute("SELECT COUNT(*) FROM cs_papers c").fetchone()[0]
        rows = cur.execute(f"SELECT * FROM cs_papers c ORDER BY {order_type} {direction} LIMIT ? OFFSET ?", (page_size, offset)).fetchall()
        
    else:
        conditions = " AND ".join(["c.academic_field LIKE ?"] * len(filters))
        params = [f"%{field}%" for field in filters]
        count = cur.execute(f"SELECT COUNT(*) FROM cs_papers c WHERE {conditions}", params).fetchone()[0]
        rows = cur.execute(f"SELECT * FROM cs_papers c WHERE {conditions} ORDER BY {order_type} {direction} LIMIT ? OFFSET ?", params + [page_size, offset]).fetchall()
    
    work_start = (page_num - 1) * 50 + 1
    if work_start > count or work_start < 1:
        raise ValueError("Page list range out of bound!")
    work_end = page_num * 50
    if work_end > count:
        work_end = count
        
    work_range = f"{work_start}–{work_end}"
    total_pages = -(-count // page_size)
    return rows, count, work_range, total_pages

con = sqlite3.connect("cs_papers.db")
res = update_results(con)
print(res[1:])
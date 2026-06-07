import sqlite3
from flask import Flask, jsonify, request, send_from_directory
import os

app = Flask(__name__, static_folder='.', static_url_path='')

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cs_papers.db')


def update_results(con, filters=None, page_num=1, order_type="citation_count", order_desc=True):
    cur = con.cursor()
    page_size = 50
    offset = (page_num - 1) * page_size
    direction = "DESC" if order_desc else "ASC"

    if not filters:
        count = cur.execute("SELECT COUNT(*) FROM cs_papers c").fetchone()[0]
        rows = cur.execute(
            f"SELECT * FROM cs_papers c ORDER BY {order_type} {direction} LIMIT ? OFFSET ?",
            (page_size, offset)
        ).fetchall()
    else:
        conditions = " AND ".join(["c.academic_field LIKE ?"] * len(filters))
        params = [f"%{field}%" for field in filters]
        count = cur.execute(
            f"SELECT COUNT(*) FROM cs_papers c WHERE {conditions}", params
        ).fetchone()[0]
        rows = cur.execute(
            f"SELECT * FROM cs_papers c WHERE {conditions} ORDER BY {order_type} {direction} LIMIT ? OFFSET ?",
            params + [page_size, offset]
        ).fetchall()

    work_start = (page_num - 1) * 50 + 1
    if work_start > count or work_start < 1:
        raise ValueError("Page list range out of bound!")
    work_end = page_num * 50
    if work_end > count:
        work_end = count

    work_range = f"{work_start}\u2013{work_end}"
    total_pages = -(-count // page_size)

    # Convert rows to list of dicts
    columns = [desc[0] for desc in cur.description]
    result_rows = []
    for row in rows:
        row_dict = {}
        for col, val in zip(columns, row):
            row_dict[col] = val
        result_rows.append(row_dict)

    return result_rows, count, work_range, total_pages


@app.route('/')
def index():
    return send_from_directory('.', 'index.html')


@app.route('/api/papers', methods=['GET'])
def get_papers():
    # Read parameters from request (stateless — client sends everything each time)
    page_num = int(request.args.get('page_num', 1))
    order_type = request.args.get('order_type', 'citation_count')
    # Validate order_type to prevent SQL injection (it's used in f-string)
    valid_order_types = ['title', 'authors', 'publication_date', 'journal_name', 'citation_count']
    if order_type not in valid_order_types:
        return jsonify({'error': 'Invalid order_type parameter'})
    order_desc = request.args.get('order_desc', 'true').lower() == 'true'

    # Parse filters: support both multiple ?filters=A&filters=B and comma-separated ?filters=A,B
    raw_filters = request.args.getlist('filters')
    filters = []
    for f in raw_filters:
        for item in f.split(','):
            item = item.strip()
            if item:
                filters.append(item)

    con = sqlite3.connect(DB_PATH)
    try:
        rows, count, work_range, total_pages = update_results(
            con, filters=filters if filters else None,
            page_num=page_num, order_type=order_type, order_desc=order_desc
        )
        return jsonify({
            'rows': rows,
            'count': count,
            'work_range': work_range,
            'total_pages': total_pages,
            'page_num': page_num,
            'filters': filters,
            'order_type': order_type,
            'order_desc': order_desc
        })
    except ValueError as e:
        return jsonify({'error': str(e), 'page_num': page_num})
    except Exception as e:
        return jsonify({'error': str(e), 'page_num': page_num})
    finally:
        con.close()


if __name__ == '__main__':
    app.run(debug=False, port=5000)

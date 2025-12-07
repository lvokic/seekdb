import random

# 样本文本
sample_texts = [
    'This is a sample fulltext content for testing purposes.',
    'Another example of fulltext content to be indexed.',
    'Fulltext search capabilities are important for many applications.',
    'Search engines rely heavily on fulltext search for document retrieval.',
    'Effective indexing techniques can improve search performance significantly.',
    'Fulltext indexing is a key feature for text-heavy applications.',
    'The power of search algorithms lies in their ability to rank content efficiently.',
    'Optimizing fulltext search indexes can lead to faster query responses.',
    'Fulltext indexes allow for faster searches on large datasets.',
    'Data retrieval systems often need robust search features to handle complex queries.'
]

# 生成 2000 条数据
def generate_insert_statements(num_records):
    insert_statements = []
    
    for i in range(1, num_records + 1):
        base_id = f'{100 + i}'  # Generate base_id as a string of the form '100', '101', ..., '2000'
        docid_col = f'{200 + i}s'  # Generate docid_col as a string like '201s', '202s', ...
        fulltext_col = random.choice(sample_texts)  # Randomly select a sample text for fulltext_col
        
        # 转义 fulltext_col 中的单引号字符
        fulltext_col_escaped = fulltext_col.replace("'", "''")  # 处理单引号转义
        
        # Generate the SQL INSERT statement for each record
        insert_statements.append(f"('{base_id}', '{docid_col}', '{fulltext_col_escaped}')")
    
    # Combine all the values into a single INSERT INTO statement
    insert_sql = f"INSERT INTO Item (base_id, docid_col, fulltext_col) VALUES\n" + ",\n".join(insert_statements) + ";"
    return insert_sql

# Generate 2000 insert statements
sql_statements = generate_insert_statements(2000)

# 保存到文件
file_path = './insert_statements.sql'
with open(file_path, 'w', encoding='utf-8') as f:
    f.write(sql_statements)

print(f"SQL 插入语句已保存到文件: {file_path}")
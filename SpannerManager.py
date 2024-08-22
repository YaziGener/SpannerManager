import tkinter as tk
from tkinter import messagebox, ttk
from google.cloud import spanner
from sklearn.linear_model import LinearRegression
import numpy as np
import time

# Configuring the Spanner Client
spanner_client = spanner.Client()
instance_id = 'test-instance'
database_id = 'test-database'
instance = spanner_client.instance(instance_id)
database = instance.database(database_id)

# Get all tables and their field information in the database
def get_table_fields():
    table_fields = {}
    field_types = {}
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_catalog = '' AND table_schema = ''")
        tables = [row[0] for row in results]

    for table in tables:
        with database.snapshot() as snapshot:
            columns = snapshot.execute_sql(f"SELECT column_name, spanner_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}' AND table_catalog = '' AND table_schema = ''")
            table_fields[table] = [row[0] for row in columns]
            field_types[table] = {row[0]: row[1] for row in columns}

    return table_fields, field_types

# Dynamically obtain all table and field information
table_fields, field_types = get_table_fields()

def browse_table():
    selected_table = browse_table_name.get()
    
    if not selected_table:
        messagebox.showerror("Error", "Please select a table to browse.")
        return

    for row in tree.get_children():
        tree.delete(row)

    query = f"SELECT * FROM {selected_table}"
    
    with database.snapshot() as snapshot:
        try:
            results = snapshot.execute_sql(query)
            rows = list(results)
            
            if not results.metadata or not rows:
                raise ValueError("Query execution failed or returned no metadata.")
            
            columns = results.metadata.row_type.fields
            tree["columns"] = [column.name for column in columns]
            
            for column in columns:
                tree.heading(column.name, text=column.name)
                tree.column(column.name, width=100)

            for row in rows:
                tree.insert("", tk.END, values=row)

        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

def update_form_fields(*args):
    selected_table = table_name.get()
    fields = table_fields.get(selected_table, [])
    
    for widget in form_frame.winfo_children():
        widget.destroy()
    
    global entries
    entries = {}
    for i, field in enumerate(fields):
        field_type = field_types[selected_table].get(field)
        tk.Label(form_frame, text=field + ":").grid(row=i, column=0, padx=5, pady=5)
        entry = tk.Entry(form_frame)
        entry.grid(row=i, column=1, padx=5, pady=5)
        if field_type == "DATE":
            entry.insert(0, date.today().isoformat())  # 自动填充当前日期
        entries[field] = entry

def insert_data():
    selected_table = table_name.get()
    fields = table_fields.get(selected_table, [])
    
    values = []
    for field in fields:
        value = entries[field].get()
        field_type = field_types[selected_table].get(field)
        if field_type == "DATE" and not value:
            value = date.today().isoformat()  # 自动填充当前日期
        values.append(value)

    if any(not value for value in values):
        messagebox.showwarning("Input Error", "All fields are required!")
        return

    try:
        with database.batch() as batch:
            batch.insert(
                table=selected_table,
                columns=tuple(fields),
                values=[tuple(values)],
            )
        messagebox.showinfo("Success", "Data inserted successfully!")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to insert data: {str(e)}")
    
    for entry in entries.values():
        entry.delete(0, tk.END)

def get_primary_key_column_and_type(table_name):
    query = f"""
    SELECT COLUMN_NAME, SPANNER_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table_name}' 
    AND ORDINAL_POSITION IN (
        SELECT ORDINAL_POSITION 
        FROM INFORMATION_SCHEMA.INDEX_COLUMNS 
        WHERE TABLE_NAME = '{table_name}' 
        AND INDEX_NAME = 'PRIMARY_KEY'
    )
    """
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(query)
        primary_key_info = [(row[0], row[1]) for row in results]
        
    if primary_key_info:
        return primary_key_info[0]  # 假设每个表只有一个主键
    else:
        raise ValueError(f"Table {table_name} does not have a primary key.")

def query_data():
    try:
        # 清除之前的查询结果
        for row in tree_query.get_children():
            tree_query.delete(row)
        
        selected_table = query_table_name.get()
        primary_key_value = primary_key_entry.get()

        if not selected_table:
            print("No table selected")
            return None

        if not primary_key_value:
            print("No primary key value provided")
            return None

        # 动态获取主键列名及其类型
        primary_key_column, primary_key_type = get_primary_key_column_and_type(selected_table)
        print(f"Primary Key Column: {primary_key_column}, Type: {primary_key_type}")
        print(f"Primary Key Value: {primary_key_value}")  # 添加这行来打印主键值

        if primary_key_type == "INT64":
            primary_key_value = int(primary_key_value)  # 确保主键值的类型正确
            param_type = spanner.param_types.INT64
        elif primary_key_type == "STRING":
            param_type = spanner.param_types.STRING
        else:
            raise ValueError(f"Unsupported primary key type: {primary_key_type}")

        query = f"SELECT * FROM {selected_table} WHERE {primary_key_column} = @primary_key_value"
        print(f"Executing SQL Query: {query}")  # 添加这行来打印SQL查询

        start_time = time.time()
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                query, 
                params={"primary_key_value": primary_key_value},
                param_types={"primary_key_value": param_type}
            )
            rows = list(results)
            end_time = time.time()

            latency = (end_time - start_time) * 1000  # 以毫秒为单位的延迟
            print(f"Query latency: {latency:.2f} ms")

            if not results.metadata or not rows:
                print("No results returned from the query")
                return None

            columns = results.metadata.row_type.fields
            column_names = [column.name for column in columns]
            tree_query["columns"] = column_names
            
            for column in columns:
                tree_query.heading(column.name, text=column.name)
                tree_query.column(column.name, width=150)

            for row in rows:
                tree_query.insert("", tk.END, values=row)

        return latency  # 成功时返回延迟值

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return None

def measure_average_latency(iterations=10):
    total_latency = 0
    valid_iterations = 0  # 用于计算成功的查询次数
    for i in range(iterations):
        print(f"Running iteration {i + 1}...")
        latency = query_data()  # 调用query_data()并获取延迟值
        print(f"Iteration {i + 1}: Latency = {latency}")  # 添加此行来打印每次迭代的延迟
        if latency is not None:  # 确保查询成功并且获取了延迟值
            total_latency += latency
            valid_iterations += 1  # 只有成功返回延迟时才增加计数器
    
    if valid_iterations > 0:
        average_latency = total_latency / valid_iterations
        print(f"Average Query Latency over {valid_iterations} iterations: {average_latency:.2f} ms")
    else:
        print("No valid query results were returned.")


    start_time = time.time()
    successful_operations = 0
    
    for i in range(iterations):
        try:
            if operation == "insert":
                insert_data()  # 调用插入数据的函数
            elif operation == "query":
                query_data()  # 调用查询数据的函数
            successful_operations += 1
        except Exception as e:
            print(f"Operation failed at iteration {i + 1}: {str(e)}")
    
    end_time = time.time()
    total_time = end_time - start_time
    throughput = successful_operations / total_time
    
    print(f"Throughput: {throughput:.2f} operations per second over {iterations} iterations.")

def measure_throughput(operation, iterations=100):
    start_time = time.time()
    successful_operations = 0
    
    for i in range(iterations):
        try:
            if operation == "insert":
                insert_data()  # 调用插入数据的函数
            elif operation == "query":
                query_data()  # 调用查询数据的函数
            successful_operations += 1
        except Exception as e:
            print(f"Operation failed at iteration {i + 1}: {str(e)}")
        
        # 每10次打印一次进度信息
        if (i + 1) % 10 == 0:
            elapsed_time = time.time() - start_time
            print(f"Completed {i + 1} operations in {elapsed_time:.2f} seconds")
    
    end_time = time.time()
    total_time = end_time - start_time
    throughput = successful_operations / total_time
    
    print(f"\nThroughput: {throughput:.2f} operations per second over {iterations} iterations.")

# Creating the Main Window
root = tk.Tk()
root.title("Google Spanner Data Manager")

# Create a Notebook (paging control)
notebook = ttk.Notebook(root)
notebook.grid(row=0, column=0, padx=10, pady=10)

# Create the frames for each page
tab_insert = ttk.Frame(notebook)
tab_query = ttk.Frame(notebook)
tab_browse = ttk.Frame(notebook)

# Adding Pagination to Notebook
notebook.add(tab_browse, text="Browse Table")
notebook.add(tab_insert, text="Insert Data")
notebook.add(tab_query, text="Query Data")

# Table selection drop-down menu - Browse table data page
tk.Label(tab_browse, text="Select Table:").grid(row=0, column=0, padx=5, pady=5)
browse_table_name = tk.StringVar(value="")
browse_table_menu = ttk.Combobox(tab_browse, textvariable=browse_table_name, values=list(table_fields.keys()))
browse_table_menu.grid(row=0, column=1, padx=5, pady=5)

browse_button = tk.Button(tab_browse, text="Browse Table", command=browse_table)
browse_button.grid(row=1, column=0, columnspan=2, pady=10)

# Data display area
tree = ttk.Treeview(tab_browse, show="headings")
tree.grid(row=2, column=0, columnspan=2, pady=10, sticky="nsew")

scrollbar = ttk.Scrollbar(tab_browse, orient="vertical", command=tree.yview)
scrollbar.grid(row=2, column=2, sticky="ns")
tree.configure(yscroll=scrollbar.set)

#********************************************************************

# Add controls in the Insert Data page
tk.Label(tab_insert, text="Select Table:").grid(row=0, column=0, padx=5, pady=5)
table_name = tk.StringVar(value="Singers")
table_name.trace("w", update_form_fields)
table_menu = ttk.Combobox(tab_insert, textvariable=table_name, values=list(table_fields.keys()))
table_menu.grid(row=0, column=1, padx=5, pady=5)

# Form Field Area
form_frame = ttk.Frame(tab_insert)
form_frame.grid(row=1, column=0, columnspan=2)

insert_button = tk.Button(tab_insert, text="Insert Data", command=insert_data)
insert_button.grid(row=2, column=0, columnspan=2, pady=10)

#********************************************************************

# In the Query Data tab, add the primary key entry field
tk.Label(tab_query, text="Select Table:").grid(row=0, column=0, padx=5, pady=5)
query_table_name = tk.StringVar(value="")
query_table_menu = ttk.Combobox(tab_query, textvariable=query_table_name, values=list(table_fields.keys()))
query_table_menu.grid(row=0, column=1, padx=5, pady=5)

# Add label and entry for the primary key
tk.Label(tab_query, text="Enter Primary Key:").grid(row=1, column=0, padx=5, pady=5)
primary_key_entry = tk.Entry(tab_query, width=50)
primary_key_entry.grid(row=1, column=1, padx=5, pady=5)

# Query button
query_button = tk.Button(tab_query, text="Query Data", command=query_data)
query_button.grid(row=2, column=0, columnspan=2, pady=10)

# Added a new "Test Latency" button in the Query Data tab
test_latency_button = tk.Button(tab_query, text="Test Latency", command=lambda: measure_average_latency(10))
test_latency_button.grid(row=4, column=0, columnspan=2, pady=10)

# Added a new "Test Throughput" button in the Query Data tab
test_throughput_button = tk.Button(tab_query, text="Test Insert Throughput", command=lambda: measure_throughput("insert", 100))
test_throughput_button.grid(row=5, column=0, columnspan=2, pady=10)

test_query_throughput_button = tk.Button(tab_query, text="Test Query Throughput", command=lambda: measure_throughput("query", 100))
test_query_throughput_button.grid(row=6, column=0, columnspan=2, pady=10)

# Data display area
tree_query = ttk.Treeview(tab_query, show="headings")
tree_query.grid(row=3, column=0, columnspan=2, pady=10, sticky="nsew")

scrollbar_query = ttk.Scrollbar(tab_query, orient="vertical", command=tree_query.yview)
scrollbar_query.grid(row=3, column=2, sticky="ns")
tree_query.configure(yscroll=scrollbar_query.set)

# Run the main loop
root.mainloop()
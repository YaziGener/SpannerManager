import pymysql
import time
import threading

# 配置 RDS 连接信息
rds_host = 'your-rds-endpoint'
username = 'your-username'
password = 'your-password'
database_name = 'your-database'

# 连接到 RDS
connection = pymysql.connect(host=rds_host,
                             user=username,
                             password=password,
                             database=database_name,
                             cursorclass=pymysql.cursors.DictCursor)

# 查询函数
def query_rds():
    try:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM your_table WHERE your_primary_key = %s"
            primary_key_value = 1  # 示例主键值
            start_time = time.time()
            cursor.execute(sql, (primary_key_value,))
            result = cursor.fetchall()
            end_time = time.time()

            latency = (end_time - start_time) * 1000  # 计算延迟，单位为毫秒
            if not result:
                print("No results returned from the query")
                return None
            return latency
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return None

# 平均延迟测试函数
def measure_average_latency_rds(iterations=10):
    total_latency = 0
    valid_iterations = 0
    for i in range(iterations):
        print(f"Running iteration {i + 1}...")
        latency = query_rds()  # 调用RDS查询函数
        if latency is not None:
            total_latency += latency
            valid_iterations += 1
    
    if valid_iterations > 0:
        average_latency = total_latency / valid_iterations
        print(f"Average Query Latency over {valid_iterations} iterations: {average_latency:.2f} ms")
    else:
        print("No valid query results were returned.")

# 吞吐量测试函数
def measure_throughput_rds(concurrent_queries=10, duration_in_seconds=10):
    def run_query():
        start_time = time.time()
        query_rds()
        end_time = time.time()
        return end_time - start_time

    start_time = time.time()
    threads = []
    completed_queries = 0
    
    while time.time() - start_time < duration_in_seconds:
        for _ in range(concurrent_queries):
            thread = threading.Thread(target=run_query)
            threads.append(thread)
            thread.start()
            completed_queries += 1
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()

    throughput = completed_queries / duration_in_seconds
    print(f"Throughput: {throughput:.2f} queries per second over {duration_in_seconds} seconds")

# 示例运行
measure_average_latency_rds(10)  # 测量平均延迟
measure_throughput_rds(concurrent_queries=100, duration_in_seconds=10)  # 测量吞吐量

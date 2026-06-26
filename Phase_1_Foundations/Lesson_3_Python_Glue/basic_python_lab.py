# basic_python_lab.py
# Beginner Level: Python Foundations for Data

print("--- 1. DATA STRUCTURES ---")
# The List (Your data row)
my_data = ["Alex", "San Francisco", 95000]
print(f"Name: {my_data[0]}")

# The Dictionary (Your JSON / Record)
my_record = {"name": "Alex", "salary": 95000}
print(f"Salary from Dict: {my_record['salary']}")

print("--- 2. INTERMEDIATE: LOOPS & LOGIC ---")
salaries = [90000, 120000, 85000, 150000]
high_earners = [s for s in salaries if s > 100000]
print(f"Architects making > 100k: {high_earners}")

print("--- 3. ARCHITECT: ERROR HANDLING ---")
try:
    # Simulating a database connection failure
    result = 10 / 0 
except ZeroDivisionError:
    print("LOG: [CRITICAL] Calculation failed! Notifying Architect...")

# 🏛️ Architect's Tip:
# "In real Data Engineering, 90% of your time is spent handling 
# errors (Data quality, Connection timeouts, API limits). Use 
# Try-Except blocks religiously to make your pipelines resilient."

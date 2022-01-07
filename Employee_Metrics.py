from sqlalchemy import create_engine, Table, MetaData, Column, Integer, String, Float
import pandas as pd

###defining functions for later use
def get_employee_orders(combined_orders, employee_id):
	'''
	This function takes in the table of all orders, with an employee id,
	and returns only the rows with orders associated with the given employee id
	'''
	s = combined_orders.select().where(combined_orders.c.Orders_EmployeeID == employee_id)
	return conn.execute(s)

def sum_employee_stats(combined_orders, employee_id):
	'''
	This function takes in the table of all orders, with an employee id,
	and returns the total number of sales made (unique order ID's),
	the total number of items sold,
	and the total currency amount of their sales
	(for the given employee id).
	'''
	Employee_Orders = get_employee_orders(combined_orders, employee_id)
	unique_orders = []
	item_total = 0
	sales_total = 0
	for row in Employee_Orders:
		item_total += row[7]
		#calculates currency amount for sale as UnitPrice*Quantity*(1-Discount)
		sales_total += row[6]*row[7]*(1-row[8])
		#adds unique order id's to the array unique_orders
		if row[0] not in unique_orders:
			unique_orders.append(row[0])
	return len(unique_orders), item_total, sales_total
 
def most_sold_item(combined_orders, employee_id):
	'''
	This function takes in the table of all orders, with an employee id,
	sums up the number of each item they sold,
	and returns the ProductID of their most sold item with its quantity
	'''
	Employee_Orders = get_employee_orders(combined_orders, employee_id)
	items_sold = {}
	for order in Employee_Orders:
		try:#attempt to add the quantity of item sold to the existing dictionary entry for that item
			items_sold[str(order[4])] += order[7]
		except KeyError:#if the entry doesn't exist, create it
			items_sold[str(order[4])] = order[7]
	highest_quantity = max(items_sold.values())
	most_sold_item_id = list(items_sold.keys())[list(items_sold.values()).index(highest_quantity)]
	return most_sold_item_id, highest_quantity
 
def most_sold_customer(combined_orders, employee_id):
	'''
	This function takes in the table of all orders, with an employee id,
	adds up the number of orders the employee made to each
	customer they sold to, and returns the customer to which they sold
	the most times and the number of times they sold.
	'''
	Employee_Orders = get_employee_orders(combined_orders, employee_id)
	customers = {}
	for order in Employee_Orders:
		try:#first try to add a tally for the customer already existing in the dictionary
			#check if the order ID of the current row is already logged in the customer's dictionary entry
			if order[0] not in customers[order[2]]:
				#add 1 to the count of orders for that customer
				customers[order[2]][0] += 1
				#append the order ID to their entry
				customers[order[2]].append(order[0])
			else:#if the order ID already exists in the dictionary entry for this customer, do nothing with this row
				pass
		except KeyError:#if customer does not exist in the dictionary, create their entry with number of orders and list of order IDs
			customers[order[2]] = [1,order[0]]
	highest_number_of_orders = max(customers.values())
	most_sold_customer_id = list(customers.keys())[list(customers.values()).index(highest_number_of_orders)]
	return most_sold_customer_id, highest_number_of_orders[0]
 
def most_sold_country(combined_orders,employee_id):
	'''
	This function takes in the table of all orders, with an employee id.
	It finds all countries the employee sold to and tallies up the
	number of sales made to that country, returning the country
	to which they sold the most times and the amount of sales.
	'''
	Employee_Orders = get_employee_orders(combined_orders, employee_id)
	countries = {}
	for order in Employee_Orders:
		try:#first try to add a tally for the country already existing in the dictionary
			#check if the order ID of the current row is already logged in the country's dictionary entry
			if order[0] not in countries[order[3]]:
				#add 1 to the count of orders for that country
				countries[order[3]][0] += 1
			else:#if the order ID already exists in the dictionary entry for that country, do nothing with this row
				pass
		except KeyError:#if country does not exist in the dictionary, create its entry with number of orders and list of order IDs
			countries[order[3]] = [1]
			countries[order[3]].append(order[0])
	highest_number_of_orders = max(countries.values())
	most_sold_country = list(countries.keys())[list(countries.values()).index(highest_number_of_orders)]
	return most_sold_country, highest_number_of_orders[0]

#########################################################################
###establishing connection with database and defining tables to draw from
meta = MetaData()
engine = create_engine('sqlite:///Northwind.db',echo=False)
conn = engine.connect()

Employees = Table(
	'Employees', meta,
	Column('EmployeeID', Integer, primary_key=True),
	Column('LastName', String),
	Column('FirstName', String)
)
 
Orders = Table(
	'Orders', meta,
	Column('OrderID', Integer, primary_key=True),
	Column('EmployeeID', Integer),
	Column('CustomerID', String),
	Column('ShipCountry', String)
)
 
Order_Details = Table(
	'Order Details', meta,
	Column('OrderID', Integer, primary_key=True),
	Column('ProductID', Integer),
	Column('UnitPrice', Float),
	Column('Quantity', Integer),
	Column('Discount', Float)
)
 
meta.create_all(engine)
 
###creating a single table with the information from Orders and Order Details tables
Combined_Orders = Orders.join(Order_Details, Orders.c.OrderID == Order_Details.c.OrderID)
 
###placing all of the employee information in a python array
s = Employees.select()
result = conn.execute(s)
employee_list=[]
for row in result:
	employee_list.append(row)
 
###creating a pandas dataframe from the python array of employees
stats = pd.DataFrame(employee_list)
stats.columns = Employees.c
 
###computing stats for each employee and adding them to the pandas dataframe
 
#computing each employee's total currency amount of sales made, the number of items they sold, and the number of orders they took
total_sales = []
number_items_sold = []
number_of_orders = []
for employee in employee_list:
	orders, items, sales = sum_employee_stats(Combined_Orders,employee[0])
	total_sales.append(sales)
	number_items_sold.append(items)
	number_of_orders.append(orders)
stats['Total Sales'] = total_sales
stats['No. Items Sold'] = number_items_sold
stats['No. of Orders'] = number_of_orders
 
#finding what item each employee sold the most of, and how many they sold
most_sold_items = []
quantities = []
for employee in employee_list:
	most_sold, quantity = most_sold_item(Combined_Orders,employee[0])
	most_sold_items.append(most_sold)
	quantities.append(quantity)
stats['Most Sold Item ID'] = most_sold_items
stats['Most Sold Item Qty'] = quantities
 
#finding what customer each employee sold to the most, and how many times they were sold to
most_sold_customers = []
sales_to_customer = []
for employee in employee_list:
	most_sold, sales_to = most_sold_customer(Combined_Orders,employee[0])
	most_sold_customers.append(most_sold)
	sales_to_customer.append(sales_to)
stats['Most Sold Cust. ID'] = most_sold_customers
stats['Most Sold Cust. Amt'] = sales_to_customer
 
#finding what country each employee sold to the most, and how many times they were sold to
most_sold_countries = []
sales_to_country = []
for employee in employee_list:
	most_sold, sales_to = most_sold_country(Combined_Orders,employee[0])
	most_sold_countries.append(most_sold)
	sales_to_country.append(sales_to)
stats['Most Sold Country'] = most_sold_countries
stats['Most Sold Country Amt'] = sales_to_country
 
###exporting pandas dataframe with all employee stats to a spreadsheet
filepath = 'Employee_Stats.xlsx'
stats.to_excel(excel_writer=filepath)

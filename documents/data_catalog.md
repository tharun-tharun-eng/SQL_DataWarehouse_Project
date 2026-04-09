# Data Dictionary for Gold Layer

---

## 📌 Overview

The Gold Layer represents the **business-ready data model**, designed for analytics and reporting.
It consists of **dimension tables** and a **fact table**, structured using a **star schema**.

---

# 🧑‍💼 1. gold.dim_customers

### 🎯 Purpose

Stores customer information enriched with demographic and geographic details.

### 📊 Columns

| Column Name     | Data Type | Description                                                   |
| --------------- | --------- | ------------------------------------------------------------- |
| customer_key    | INT       | Surrogate key uniquely identifying each customer record.      |
| customer_id     | INT       | Unique identifier from source system.                         |
| customer_number | NVARCHAR  | Business key representing the customer.                       |
| first_name      | NVARCHAR  | Customer's first name.                                        |
| last_name       | NVARCHAR  | Customer's last name.                                         |
| cntry           | NVARCHAR  | Country of the customer.                                      |
| marital_status  | NVARCHAR  | Customer's marital status (Single, Married, etc.).            |
| gender          | NVARCHAR  | Standardized gender value (CRM preferred, else ERP fallback). |
| bdate           | DATE      | Customer's birthdate.                                         |
| cst_create_date | DATE      | Date when customer record was created.                        |

---

# 📦 2. gold.dim_products

### 🎯 Purpose

Stores product details including category hierarchy and pricing information.

### 📊 Columns

| Column Name        | Data Type | Description                           |
| ------------------ | --------- | ------------------------------------- |
| product_key        | INT       | Surrogate key for each product.       |
| product_id         | INT       | Unique identifier from source system. |
| product_number     | NVARCHAR  | Business key for product.             |
| product_name       | NVARCHAR  | Name of the product.                  |
| category_id        | NVARCHAR  | Category identifier.                  |
| category           | NVARCHAR  | Product category (e.g., Bikes).       |
| subcategory        | NVARCHAR  | Product subcategory.                  |
| maintenance        | NVARCHAR  | Maintenance classification.           |
| product_cost       | INT       | Cost of the product.                  |
| product_line       | NVARCHAR  | Product line (Mountain, Road, etc.).  |
| product_start_date | DATE      | Product validity start date.          |

---

# 💰 3. gold.fact_sales

### 🎯 Purpose

Stores transactional sales data linking customers and products.

### 📊 Grain

One record per **order line (product × customer × order)**

### 📊 Columns

| Column Name   | Data Type | Description                   |
| ------------- | --------- | ----------------------------- |
| order_number  | NVARCHAR  | Unique order identifier.      |
| product_key   | INT       | Foreign key to dim_products.  |
| customer_id   | INT       | Foreign key to dim_customers. |
| order_date    | DATE      | Date when order was placed.   |
| shipping_date | DATE      | Date when order was shipped.  |
| due_date      | DATE      | Expected delivery date.       |
| sales_amount  | INT       | Total sales amount.           |
| quantity      | INT       | Number of items sold.         |
| sls_price     | INT       | Price per unit.               |

---

# 🔗 Relationships

* `fact_sales.product_key → dim_products.product_key`
* `fact_sales.customer_id → dim_customers.customer_id`

---

# 📈 Example Queries

### Total Sales by Product

```sql
SELECT p.product_name, SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_name;
```

### Total Sales by Country

```sql
SELECT c.cntry, SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.cntry;
```

---

# 👨‍💻 Author

Tharun Kumar

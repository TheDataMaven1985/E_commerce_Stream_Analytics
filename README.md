# E-commerce Real-Time Streaming Analytics 🛍️📊

A real-time data streaming and analytics system built with Apache Spark that simulates e-commerce transactions and provides instant business insights.

## 🎯 Project Overview

This project demonstrates end-to-end streaming data engineering using PySpark to process and analyze e-commerce transactions in real-time. It simulates a live e-commerce platform with multiple analytics streams providing actionable business intelligence.

## ✨ Features

- **Real-time Data Streaming**: Socket-based transaction data generation
- **6 Analytics Streams**:
  - 📈 Sales Dashboard (overall metrics)
  - 📦 Category Analysis (product categories performance)
  - 🌍 Geographic Sales (regional insights)
  - 🏆 Top Products (windowed aggregations)
  - ⚠️ Fraud Detection (high-value transaction alerts)
  - 💳 Payment Method Analysis

## 🛠️ Tech Stack

- **Apache Spark 3.5.3** - Distributed data processing
- **PySpark** - Python API for Spark
- **Python 3.11+** - Core programming language
- **Socket Programming** - Data streaming
- **Structured Streaming** - Real-time processing

## 📁 Project Structure
```
E-commerce_Stream_Analytics/
├── ecommerce_sender.py          # Data generator (socket server)
├── ecommerce_analytics.py       # Spark streaming consumer          
└── .gitignore                    # Git ignore rules
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Java 11 or 17 (for Spark)
- pip

### Installation

1. **Clone the repository**
```bash
   git clone https://github.com/TheDataMaven1985/E_commerce_Stream_Analytics.git
   cd E_commerce_Stream_Analytics
```

2. **Create virtual environment**
```bash
   python -m venv venv
   
   # Activate
   # Windows:
   .\venv\Scripts\Activate
   
   # Linux/Mac:
   source venv/bin/activate
```

3. **Install dependencies**
```bash
   pip install pyspark==3.5.3
```

4. **Set up Java** (if not already installed)
   - Download Java 11 from [Adoptium](https://adoptium.net/)
   - Set `JAVA_HOME` environment variable

### Running the Application

You need **two terminals**:

**Terminal 1 - Data Sender:**
```bash
python ecommerce_sender.py
```

**Terminal 2 - Analytics Consumer:**
```bash
python ecommerce_analytics.py
```

## 📊 Sample Output

### Sales Dashboard
```
+------------------+-------------+---------------------+
|total_transactions|total_revenue|avg_transaction_value|
+------------------+-------------+---------------------+
|150               |12,450.67    |83.00                |
+------------------+-------------+---------------------+
```

### Fraud Alerts
```
+----------------+-----------+------------+--------------+
|transaction_id  |customer_id|total_amount|product_name  |
+----------------+-----------+------------+--------------+
|TXN1766715558675|CUST0354   |400.0       |Television    |
+----------------+-----------+------------+--------------+
```

## 🎓 What I Learned

- Building end-to-end streaming data pipelines
- Apache Spark Structured Streaming concepts
- Real-time aggregations and windowing operations
- Watermarking for late-arriving data
- Multiple concurrent streaming queries
- Socket programming for data transfer
- Production-ready streaming architecture patterns

## 🔮 Future Enhancements

- [ ] Replace sockets with Apache Kafka
- [ ] Add machine learning for anomaly detection
- [ ] Create interactive dashboard with Dash/Streamlit
- [ ] Implement data quality monitoring
- [ ] Add unit tests
- [ ] Deploy on cloud (AWS/Azure)
- [ ] Add customer segmentation analysis
- [ ] Real-time recommendations engine

## 📝 Key Concepts Demonstrated

- **Streaming Data Sources**: Socket, Kafka-ready architecture
- **Stateful Processing**: Windowed aggregations with watermarks
- **Output Modes**: Complete, Append, Update
- **Trigger Intervals**: Configurable batch processing
- **Data Quality**: Schema validation and error handling

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 👤 Author

**Favour Kolawole**
- GitHub: [@TheDataMaven1985](https://github.com/TheDataMaven1985)

## 🙏 Acknowledgments

- Apache Spark documentation
- PySpark Structured Streaming guides
- Real-world e-commerce analytics patterns

---

⭐ If you found this project helpful, please give it a star!

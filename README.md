# 🏪 KICKZ EMPIRE — ELT Pipeline

ELT (Extract, Load, Transform) pipeline for the **KICKZ EMPIRE** e-commerce platform, built as part of the IMT Atlantique Data Engineering course.

---

## 📖 Project Description

KICKZ EMPIRE is a fast-growing e-commerce platform selling sneakers and streetwear.  
Raw data (orders, users, products, reviews, clickstream) is stored in an S3 data lake but cannot be easily queried.

This project implements a full **ELT pipeline** to:
- Extract raw data from S3
- Load it into PostgreSQL
- Transform and clean it
- Produce business-ready analytics tables

It enables teams to answer key questions such as:
- Daily revenue 📊
- Top products 🏆
- Customer lifetime value 💰

---

## 🏗️ Architecture

| Layer | Description |
|------|-------------|
| **Bronze** | Raw data copied as-is from S3 |
| **Silver** | Cleaned data (no PII, validated types) |
| **Gold** | Aggregated tables for business insights |

---

## ⚙️ Setup Instructions

```bash
# 1. Clone repo
git clone <repo-url>
cd <repo>

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate   # Linux/Mac
# .\venv\Scripts\activate  # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
```

## 🚀 How to Run

▶️ Full pipeline
```python pipeline.py```

▶️ Step by step
```bash
# Extract → Bronze
python pipeline.py --step extract

# Transform → Silver
python pipeline.py --step transform

# Gold layer (analytics)
python pipeline.py --step gold
```
---

## 🧪 How to Test
```bash
# Run tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```
---

## 🔁 CI/CD

A GitHub Actions pipeline automatically runs on each push:
- Linting (flake8)
- Tests (pytest)
- Coverage report

Ensures code quality and reliability before deployment.

---

## 📊 Monitoring

Each pipeline run generates a pipeline_report.json with:
- Step status (success/failure)
- Duration
- Rows processed
- Errors (if any)

---

## 🛠️ Tech Stack
- ython 3.10+
- pandas
- boto3 (AWS S3)
- SQLAlchemy (PostgreSQL)
- pytest (testing)
- GitHub Actions (CI/CD)

---

## 👥 Team Members
Louise DELFOSSE - Pol GUILLOU - Ethân PERSONNAZ
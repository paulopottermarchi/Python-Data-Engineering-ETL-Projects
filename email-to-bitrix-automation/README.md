# 📧 Email-to-Bitrix Payment Automation

A real-world automation pipeline deployed in a **production environment** at MBA Serviços de Cobranças (Brazil), automating daily payment reporting for debt collection portfolios.

---

## 🔍 Problem

The finance team received daily payment report emails from the Debthor debt collection platform, each containing an Excel attachment with transaction records. Manually reading these emails, summing the values, and posting updates to the team's Bitrix24 group was a repetitive and error-prone task performed every day.

---

## ✅ Solution

A Python automation pipeline that:

1. Connects to the corporate IMAP email server
2. Filters unread emails by client subject line and current date
3. Downloads and parses the `.xlsx` attachment
4. Aggregates all payment values
5. Posts a formatted summary message to the Bitrix24 group feed via REST API
6. Marks the email as read to prevent duplicate processing
7. Runs automatically every 15 minutes via Windows Task Scheduler

---

## 📊 Pipeline Architecture

```
Corporate Email Inbox (IMAP)
         │
         ▼
  Filter Unread Emails
  by Subject + Date (UNSEEN)
         │
         ▼
  Download .xlsx Attachment
         │
         ▼
  Parse & Aggregate
  Payment Values (openpyxl)
         │
         ▼
  Format Message
         │
         ▼
  POST to Bitrix24
  Group Feed (REST API)
         │
         ▼
  Mark Email as Read
```

---

## 💬 Output Example

```
Payment report from the client 501 for amount of R$ 1.240,75 from 13.04.2026 received and uploaded in ANT
```

---

## 🛠️ Technologies

| Technology | Usage |
|---|---|
| Python 3.13 | Core language |
| imaplib | IMAP email connection |
| openpyxl | Excel file parsing |
| requests | Bitrix24 REST API calls |
| python-dotenv | Environment variable management |
| Windows Task Scheduler | Automated execution every 15 minutes |

---

## 📁 Project Structure

```
email-to-bitrix-automation/
│
├── main.py           # Main pipeline script
├── rodar.vbs         # Windows Script Host launcher for Task Scheduler
├── .env.example      # Environment variables template
├── .gitignore        # Excludes .env and sensitive files
└── README.md
```

---

## ⚙️ Setup & Usage

### 1. Clone the repository
```bash
git clone https://github.com/your-username/data-engineering-monorepo.git
cd data-engineering-monorepo/email-to-bitrix-automation
```

### 2. Install dependencies
```bash
pip install openpyxl requests python-dotenv
```

### 3. Configure environment variables
```bash
cp .env.example .env
```

Edit `.env` with your credentials:
```
EMAIL=your_email@company.com
PASSWORD=your_password
IMAP_SERVER=mail.yourserver.com
BITRIX_WEBHOOK=https://yourdomain.bitrix24.com/rest/ID/TOKEN/
BITRIX_GROUP_ID=000
```

### 4. Add clients
Edit the `CLIENTES` list in `main.py`:
```python
CLIENTES = [
    {"subject": "Payment import of client Crediativos", "id": "501"},
    {"subject": "Payment import of client Dentalpar 516", "id": "516"},
]
```

### 5. Run manually
```bash
python main.py
```

### 6. Schedule automation (Windows)
- Open **Task Scheduler**
- Create a new task pointing to `rodar.vbs`
- Program/script: `wscript.exe`
- Arguments: `"C:\path\to\EmailAutomation\rodar.vbs"`
- Set trigger to repeat every **15 minutes**

---

## 🔒 Security

- All credentials are stored in a `.env` file
- `.env` is listed in `.gitignore` and never committed to the repository
- Use `.env.example` as a safe template for onboarding

---

## 🏭 Production Context

This pipeline was built and deployed at **MBA Serviços de Cobranças Ltda**, a Brazilian debt collection company operating the Debthor platform. It processes daily payment imports across multiple client portfolios including:

- Crediativos (Client 501)
- Dentalpar (Client 516)

The automation replaced a fully manual reporting process, reducing human error and ensuring real-time visibility for the operations team.

---

## 📈 Concepts Explored

- Email automation via IMAP protocol
- Excel data extraction and aggregation
- REST API integration (Bitrix24)
- Environment variable management
- Multi-client pipeline configuration
- Windows Task Scheduler deployment via VBScript
- Production pipeline monitoring

---

## 👤 Author

**Paulo Potter Marchi**
MIS Analyst | Analytics Engineer
Transitioning into Data Engineering
📍 Brazil → 🇬🇧 UK (2026)

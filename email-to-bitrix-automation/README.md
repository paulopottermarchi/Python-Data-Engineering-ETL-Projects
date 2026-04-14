## Email-to-Bitrix Payment Automation

A real-world automation pipeline that monitors a corporate email inbox for 
payment report emails, extracts financial data from Excel attachments, and 
posts formatted summaries to a Bitrix24 group feed.

Pipeline steps:
1. Connect to corporate IMAP server and filter unread emails by subject
2. Extract and parse `.xlsx` attachment using openpyxl
3. Aggregate payment values and extract transaction date
4. Format and post message to Bitrix24 via REST API webhook
5. Mark email as read to prevent duplicate processing

Concepts explored:
- Email automation via IMAP
- Excel data extraction (openpyxl)
- REST API integration (Bitrix24)
- Environment variable management (.env / dotenv)
- Windows Task Scheduler automation
- Multi-client pipeline configuration

> This project was built and deployed in a **production environment** at MBA 
> Serviços de Cobranças, automating daily payment reporting for debt collection 
> portfolios (clients 501, 516).

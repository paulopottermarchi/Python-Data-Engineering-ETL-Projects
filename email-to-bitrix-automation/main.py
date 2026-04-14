import imaplib
import email
import openpyxl
import requests
import io
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# ─── CONFIGURAÇÕES ───────────────────────────────────────────────
OUTLOOK_EMAIL    = os.getenv("EMAIL")
OUTLOOK_PASSWORD = os.getenv("PASSWORD")
IMAP_SERVER      = os.getenv("IMAP_SERVER")

BITRIX_WEBHOOK   = os.getenv("BITRIX_WEBHOOK")
BITRIX_GROUP_ID  = int(os.getenv("BITRIX_GROUP_ID"))

# ─── CLIENTES — adicione novos aqui ──────────────────────────────
CLIENTES = [
    {"subject": "Payment import of client Crediativos", "id": "501"},
    {"subject": "Payment import of client Dentalpar 516", "id": "516"},
]
# ──────────────────────────────────────────────────────────────────


def conectar_outlook():
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(OUTLOOK_EMAIL, OUTLOOK_PASSWORD)
    mail.select("INBOX")
    return mail


def buscar_emails(mail, subject):
    """Busca emails não lidos de hoje com o assunto informado."""
    hoje = datetime.now().strftime("%d-%b-%Y")
    _, msgs = mail.search(None, f'(UNSEEN) (SUBJECT "{subject}") (SINCE {hoje})')
    ids = msgs[0].split()

    for num in reversed(ids):
        _, data = mail.fetch(num, "(RFC822)")
        msg = email.message_from_bytes(data[0][1])

        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            filename = part.get_filename()
            if filename and filename.endswith(".xlsx"):
                payload = part.get_payload(decode=True)
                yield num, filename, io.BytesIO(payload)


def processar_xlsx(arquivo_bytes):
    """Lê o xlsx e retorna total e data."""
    wb = openpyxl.load_workbook(arquivo_bytes, read_only=True)
    ws = wb.active

    total = 0.0
    data_transacao = None

    for row in ws.iter_rows(min_row=2, values_only=True):
        dt  = row[2]  # DT_TRANSACAO
        val = row[3]  # CR_VALOR

        if val is not None:
            total += float(str(val).replace(",", "."))
        if dt and data_transacao is None:
            data_transacao = str(dt)

    return total, data_transacao


def formatar_valor(total):
    return f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def postar_no_bitrix(mensagem):
    url = BITRIX_WEBHOOK + "log.blogpost.add"
    payload = {
        "POST_TITLE": "Payment Report",
        "POST_MESSAGE": mensagem,
        "DEST": [f"SG{BITRIX_GROUP_ID}"],
    }
    resp = requests.post(url, json=payload)
    return resp.json()


def main():
    mail = conectar_outlook()

    for cliente in CLIENTES:
        print(f"\nVerificando cliente {cliente['id']}...")

        for num_email, filename, xlsx_bytes in buscar_emails(mail, cliente["subject"]):
            print(f"Processando: {filename}")

            total, data_tx = processar_xlsx(xlsx_bytes)
            valor_fmt      = formatar_valor(total)

            mensagem = (
                f"Payment report from the client {cliente['id']} "
                f"for amount of {valor_fmt} "
                f"from {data_tx} "
                f"received and uploaded in ANT"
            )

            print(f"Mensagem: {mensagem}")
            resultado = postar_no_bitrix(mensagem)
            print(f"Bitrix response: {resultado}")

            mail.store(num_email, "+FLAGS", "\\Seen")

    mail.logout()


if __name__ == "__main__":
    main()

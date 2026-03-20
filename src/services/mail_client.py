"""Mail client with separate classes for SMTP, IMAP, and Outlook access."""
import os
import smtplib
import imaplib
import logging
import mimetypes
import email
from email.message import EmailMessage
from config import EMAIL_ADDRESS, EMAIL_APP_PASSWORD

logger = logging.getLogger(__name__)


class SMTPClient:
    """Send emails via SMTP with optional context manager for persistent connection."""
    
    def __init__(self, from_email: str = None, password: str = None, smtp_server: str = None):
        self.from_email = from_email or EMAIL_ADDRESS
        self.password = password or EMAIL_APP_PASSWORD
        self.smtp_server = smtp_server or "smtp.office365.com"
        self.server = None
    
    def __enter__(self):
        """Establish persistent SMTP connection."""
        if not all([self.from_email, self.password]):
            raise ValueError("Missing EMAIL_ADDRESS or EMAIL_APP_PASSWORD")
        try:
            self.server = smtplib.SMTP(self.smtp_server, 587)
            self.server.starttls()
            self.server.login(self.from_email, self.password)
        except Exception as e:
            logger.error(f"SMTP connection failed: {e}")
            raise
        return self
    
    def __exit__(self, *args):
        if self.server:
            try:
                self.server.quit()
            except Exception as e:
                logger.warning(f"SMTP close error: {e}")
    
    def send_email(
        self,
        subject: str,
        body: str,
        to_email: list[str] | str,
        cc_emails: list[str] | str = None,
        bcc_emails: list[str] | str = None,
        attachments: list[str] = None,
    ) -> bool:
        """Send email. Returns True on success, False otherwise."""
        if not all([self.from_email, self.password]):
            logger.error("Missing email credentials")
            return False
        
        try:
            to = self._parse_recipients(to_email)
            cc = self._parse_recipients(cc_emails)
            bcc = self._parse_recipients(bcc_emails)
            
            if not (to or cc or bcc):
                logger.error("No recipients provided")
                return False
            
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = self.from_email
            if to:
                msg["To"] = ", ".join(to)
            if cc:
                msg["Cc"] = ", ".join(cc)
            msg.set_content(body)
            
            if not self._attach_files(msg, attachments or []):
                return False
            
            server = self.server or self._create_connection()
            server.send_message(msg, to_addrs=to + cc + bcc)
            
            logger.info(f"Email sent: {len(to)} to, {len(cc)} cc, {len(bcc)} bcc")
            return True
            
        except Exception as e:
            logger.error(f"Send failed: {e}")
            return False
    
    def _parse_recipients(self, value: list[str] | str = None) -> list[str]:
        """Convert string or list to normalized email list."""
        if not value:
            return []
        if isinstance(value, str):
            return [e.strip() for e in value.split(",") if e.strip()]
        return [str(e).strip() for e in value if e.strip()]
    
    def _attach_files(self, msg: EmailMessage, paths: list[str]) -> bool:
        """Attach files. Returns False if any file not found."""
        for path in paths:
            if not os.path.isfile(path):
                logger.error(f"File not found: {path}")
                return False
            mime_type, _ = mimetypes.guess_type(path)
            maintype, subtype = (mime_type.split("/", 1) 
                               if mime_type else ("application", "octet-stream"))
            with open(path, "rb") as f:
                msg.add_attachment(f.read(), maintype=maintype, subtype=subtype,
                                 filename=os.path.basename(path))
        return True
    
    def _create_connection(self):
        """Create temporary SMTP connection."""
        server = smtplib.SMTP(self.smtp_server, 587)
        server.starttls()
        server.login(self.from_email, self.password)
        return server


class IMAPReader:
    """Read emails from server via IMAP protocol."""
    
    def __init__(self, from_email: str = None, password: str = None, smtp_server: str = None):
        self.from_email = from_email or EMAIL_ADDRESS
        self.password = password or EMAIL_APP_PASSWORD
        self.smtp_server = smtp_server or "smtp.office365.com"
    
    def get_emails(self, mailbox: str = "INBOX", limit: int = 10) -> list[dict]:
        """Fetch recent emails from server.
        
        Returns: List of dicts with keys: subject, sender, body, date.
        """
        if not all([self.from_email, self.password]):
            logger.error("Missing email credentials")
            return []
        
        try:
            imap_server = self._get_imap_server()
            imap = imaplib.IMAP4_SSL(imap_server)
            imap.login(self.from_email, self.password)
            imap.select(mailbox)
            
            status, email_ids = imap.search(None, "ALL")
            if status != "OK" or not email_ids[0]:
                logger.info(f"No emails found in {mailbox}")
                imap.close()
                return []
            
            email_list = []
            for email_id in email_ids[0].split()[-limit:]:
                status, msg_data = imap.fetch(email_id, "(RFC822)")
                if status != "OK":
                    continue
                
                msg = EmailMessage._from_string(msg_data[0][1].decode())
                email_list.append({
                    "subject": msg.get("Subject", ""),
                    "sender": msg.get("From", ""),
                    "body": msg.get_content(),
                    "date": msg.get("Date", ""),
                })
            
            imap.close()
            logger.info(f"Retrieved {len(email_list)} emails from {mailbox}")
            return email_list
            
        except Exception as e:
            logger.error(f"Failed to retrieve emails: {e}")
            return []
    
    def get_all_emails_with_attachments(self, mailbox: str = "INBOX"):
        """Yield raw email messages for manual processing.
        
        Yields: Email message objects with attachment access.
        """
        if not all([self.from_email, self.password]):
            raise ValueError("Missing EMAIL_ADDRESS or EMAIL_APP_PASSWORD")
        
        try:
            imap = imaplib.IMAP4_SSL(self._get_imap_server())
            imap.login(self.from_email, self.password)
            imap.select(mailbox)
            
            status, email_ids = imap.search(None, "ALL")
            if status != "OK":
                logger.warning("No emails found")
                imap.close()
                return
            
            for email_id in email_ids[0].split():
                try:
                    status, msg_data = imap.fetch(email_id, "(RFC822)")
                    if status == "OK":
                        yield email.message_from_bytes(msg_data[0][1])
                except Exception as e:
                    logger.warning(f"Error processing email {email_id}: {e}")
            
            imap.close()
            
        except Exception as e:
            logger.error(f"Failed to connect to mailbox: {e}")
            raise
    
    def _get_imap_server(self) -> str:
        """Get IMAP server address based on SMTP server."""
        smtp_to_imap = {
            "smtp.office365.com": "imap.office365.com",
            "smtp.gmail.com": "imap.gmail.com",
        }
        return smtp_to_imap.get(self.smtp_server, self.smtp_server.replace("smtp.", "imap."))


class OutlookReader:
    """Read emails directly from local Outlook application (Windows only)."""
    
    def get_emails_from_folder(self, folder_name: str = "INBOX", limit: int = 10):
        """Fetch emails directly from Outlook app (local, no network).
        
        Args:
            folder_name: Outlook folder name (e.g., "INBOX", "Confirmations")
            limit: Max emails to process
        
        Yields: Outlook mail item objects with .Attachments property
        
        Requires: Outlook desktop app running, win32com installed (pip install pywin32)
        """
        try:
            import win32com.client
        except ImportError:
            raise ImportError("win32com not installed. Run: pip install pywin32")
        
        try:
            outlook = win32com.client.Dispatch("Outlook.Application")
            namespace = outlook.GetNamespace("MAPI")
            
            folder = self._get_outlook_folder(namespace, folder_name)
            
            count = 0
            for item in folder.Items:
                if count >= limit:
                    break
                if item.Class == 43:  # 43 = Mail item
                    try:
                        yield item
                        count += 1
                    except Exception as e:
                        logger.warning(f"Error processing outlook item: {e}")
            
            logger.info(f"Retrieved {count} emails from Outlook folder '{folder_name}'")
            
        except Exception as e:
            logger.error(f"Failed to access Outlook: {e}")
            raise
    
    def _get_outlook_folder(self, namespace, folder_name: str):
        """Get Outlook folder by name, handling both default and custom folders."""
        if folder_name == "INBOX":
            return namespace.GetDefaultFolder(6)  # 6 = Inbox
        
        try:
            return namespace.Folders.Item(1).Folders(folder_name)
        except:
            for f in namespace.Folders:
                if f.Name == folder_name:
                    return f
            raise ValueError(f"Folder '{folder_name}' not found in Outlook")


# Convenience aliases for backwards compatibility
MailClient = SMTPClient

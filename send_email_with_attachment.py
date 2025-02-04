import json
import requests

def send_email_with_attachment(subject, body, recipients, attachment_path):
    try:
        api_url = 'https://openapi.stage.samsung.net/mail/api/v2.0/mails/send?userId=dsllm_dp.sec'
        headers = {
            'Authorization': 'Bearer 51681be2-43bc-3eb5-9e80-03d732437542',
            'System-ID': 'KCC10REST02233'
        }
        
        mail_payload = {
            "subject": subject,
            "contents": body,
            "contentType": "TEXT",
            "docSecuType": "PERSONAL",
            "sender": {
                "emailAddress": "dsllm_dp.sec@stage.samsung.com"
            },
            "recipients": [
                {"emailAddress": recipient, "recipientType": "TO"} for recipient in recipients
            ]
        }
        
        with open(attachment_path, 'rb') as attachment_file:
            files = {
                'attachment': attachment_file,
                'mail': (None, json.dumps(mail_payload))  # Ensure 'mail' is sent as a JSON string
            }
            
            response = requests.post(api_url, headers=headers, files=files)
        
        response.raise_for_status()
        print("Email sent successfully")
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to send email: {e}")
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    subject = "EMAIL SUBJECT (이메일 제목)"
    body = "EMAIL BODY (이메일 본문)"
    recipients = ["dsllm_dp.sec@stage.samsung.com", "sejin78.yun@stage.samsung.com"]
    attachment_path = "3k356bBts/github-recovery-codes.txt"
    send_email_with_attachment(subject, body, recipients, attachment_path)

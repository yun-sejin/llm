import requests

def send_email(subject, body, recipients):
    api_url = 'https://openapi.stage.samsung.net/mail/api/v2.0/mails/send?userId=dsllm_dp.sec'
    headers = {
        'Authorization': 'Bearer 51681be2-43bc-3eb5-9e80-03d732437542',
        'System-ID': 'KCC10REST02233',
        'Content-Type': 'application/json'
    }
    
    payload = {
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
    response = requests.post(api_url, headers=headers, json=payload)
    if response.status_code == 200:
        print("Email sent successfully")
    else:
        print(f"Failed to send email. Status code: {response.status_code}, Response: {response.text}")

if __name__ == "__main__":
    subject = "EMAIL SUBJECT (이메일 제목)"
    body = "EMAIL BODY (이메일 본문)"
    recipients = ["dsllm_dp.sec@stage.samsung.com", "sejin78.yun@stage.samsung.com"]
    send_email(subject, body, recipients)

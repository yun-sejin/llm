import json
import requests
from airflow.models import Variable 

def send_email_with_attachment_rest_api(subject, body, recipients, attachment_path):
    try:
        user_id = 'dsllm_dp.sec'
        api_url = Variable.get('EMAIL_API_URL').format(user_id=user_id)
        # api_url = Variable.get('EMAIL_API_URL').format(user_id=user_id)
        # api_url = os.getenv("EMAIL_API_URL")
        # auth_token = os.getenv("EMAIL_API_TOKEN")
        # 환경 변수 사용: 민감한 정보(예: api_url, Authorization 토큰 등)는 코드에 직접 포함하지 않고 환경 변수를 사용하는 것이 보안에 좋습니다.
        auth_token = '51681be2-43bc-3eb5-9e80-03d732437542'
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'System-ID': 'KCC10REST02233'
            #  'Content-Type': 'multipart/form-data'
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
                'mail': (None, json.dumps(mail_payload), 'application/json')
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
    send_email_with_attachment_rest_api(subject, body, recipients, attachment_path)
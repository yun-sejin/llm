from fastapi import FastAPI, File, UploadFile, HTTPException
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

app = FastAPI()

# Define maximum file sizes for different zip formats
MAX_FILE_SIZES = {
    'zip': 100 * 1024 * 1024,  # 100 MB
    'tar': 200 * 1024 * 1024,  # 200 MB
    'gz': 50 * 1024 * 1024,    # 50 MB
}

def get_max_file_size(file_extension):
    return MAX_FILE_SIZES.get(file_extension, 0)

@app.post("/upload-zip/")
async def upload_zip(file: UploadFile = File(...)):
    file_extension = file.filename.split('.')[-1]
    max_file_size = get_max_file_size(file_extension)
    
    if file.spool_max_size > max_file_size:
        raise HTTPException(status_code=400, detail=f"File size exceeds the maximum allowed size for {file_extension} files.")
    
    try:
        s3 = boto3.client('s3')
        bucket_name = 'your-bucket-name'
        s3.upload_fileobj(file.file, bucket_name, file.filename)
        return {"filename": file.filename, "max_file_size": max_file_size}
    except (NoCredentialsError, PartialCredentialsError):
        raise HTTPException(status_code=500, detail="S3 credentials not available.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



업로드 시 502와 500 에러가 발생하는 이유는 여러 가지가 있을 수 있습니다. 아래는 몇 가지 일반적인 원인과 해결 방법입니다:

S3 인증 문제:

Boto3 클라이언트가 S3에 접근할 수 있는 자격 증명이 올바르지 않을 경우 500 에러가 발생할 수 있습니다.
해결 방법: AWS 자격 증명이 올바른지 확인하고, 환경 변수나 AWS 설정 파일을 통해 자격 증명을 설정합니다.
파일 크기 제한 문제:

파일 크기가 설정된 최대 크기를 초과할 경우 400 에러가 발생할 수 있습니다.
해결 방법: 파일 크기를 확인하고, 설정된 최대 크기 이하인지 확인합니다.
네트워크 문제:

네트워크 연결 문제로 인해 S3에 파일을 업로드할 수 없을 경우 502 에러가 발생할 수 있습니다.
해결 방법: 네트워크 연결 상태를 확인하고, S3 서비스가 정상적으로 동작하는지 확인합니다.
FastAPI 설정 문제:

FastAPI 설정이나 코드에 문제가 있을 경우 500 에러가 발생할 수 있습니다.
해결 방법: FastAPI 로그를 확인하고, 코드에 오류가 없는지 확인합니다.
파일 핸들링 문제:

파일을 제대로 읽거나 처리하지 못할 경우 500 에러가 발생할 수 있습니다.
해결 방법: 파일을 제대로 읽고 처리하는지 확인합니다.
아래는 위의 문제를 해결하기 위한 코드 예시입니다:
    
        from fastapi import FastAPI, File, UploadFile, HTTPException
    import boto3
    from botocore.exceptions import NoCredentialsError, PartialCredentialsError
    
    app = FastAPI()
    
    MAX_FILE_SIZES = {
        'zip': 100 * 1024 * 1024,  # 100 MB
        'tar': 200 * 1024 * 1024,  # 200 MB
        'gz': 50 * 1024 * 1024,    # 50 MB
    }
    
    def get_max_file_size(file_extension):
        return MAX_FILE_SIZES.get(file_extension, 0)
    
    @app.post("/upload-zip/")
    async def upload_zip(file: UploadFile = File(...)):
        file_extension = file.filename.split('.')[-1]
        max_file_size = get_max_file_size(file_extension)
        
        if file.spool_max_size > max_file_size:
            raise HTTPException(status_code=400, detail=f"File size exceeds the maximum allowed size for {file_extension} files.")
        
        try:
            s3 = boto3.client('s3')
            bucket_name = 'your-bucket-name'
            s3.upload_fileobj(file.file, bucket_name, file.filename)
            return {"filename": file.filename}
        except NoCredentialsError:
            raise HTTPException(status_code=500, detail="AWS credentials not found.")
        except PartialCredentialsError:
            raise HTTPException(status_code=500, detail="Incomplete AWS credentials.")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        
        Nginx 설정을 변경하여 업로드 파일 크기를 50기가바이트로 설정하려면 client_max_body_size 지시어를 사용해야 합니다. 이 지시어는 Nginx가 허용하는 최대 요청 본문 크기를 설정합니다.

Nginx 설정 파일 열기:

일반적으로 Nginx 설정 파일은 /etc/nginx/nginx.conf 또는 /etc/nginx/conf.d/default.conf에 위치합니다.
client_max_body_size 지시어 추가 또는 수정:

설정 파일의 http, server, 또는 location 블록에 client_max_body_size 지시어를 추가하거나 수정합니다.

http {
    ...
    client_max_body_size 50G;
    ...
}

server {
    ...
    client_max_body_size 50G;
    ...
}

location / {
    ...
    client_max_body_size 50G;
    ...
}

Nginx 설정 파일 저장 후 Nginx 재시작:
sudo nginx -t  # 설정 파일 문법 검사
sudo systemctl restart nginx  # Nginx 재시작
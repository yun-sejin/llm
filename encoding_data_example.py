import json
import re

# Sample data with non-ASCII characters; updated "message" to include a null byte
data = {
    "message": "안녕하세요\x00",  # Appended null character (0x00)
    "value": 123,
    "page_str_list": ["페이지1\x00", "페이지2", "페이지3"],
}

# Convert data to a JSON string without ASCII escaping and encode as bytea (bytes)
json_data = json.dumps(data, ensure_ascii=False)
json_bytes = json_data.encode('utf-8')

# Replace all 0x00 bytes with a space using regex
json_bytes = re.sub(b'\x00', b' ', json_bytes)

# Print the result
print(json_bytes)

def decode_json_bytes(encoded_bytes):
    """
    Decode bytea data by decoding UTF-8 bytes back to a JSON string and parsing it into a Python object.
    """
    decoded_str = re.sub(b'\x00', b' ', encoded_bytes).decode('utf-8')
    # decoded_str = encoded_bytes.decode('utf-8')
    return json.loads(decoded_str)

def tobytes_json_data(encoded_bytes):
    """
    Return the encoded JSON data as bytes using the tobytes() method.
    """
    mdata = memoryview(encoded_bytes)
    # dcdata = mdata.tobytes()
    # dcdata = re.sub(b'\x00', b' ', dcdata)
    # loda_data = json.loads(dcdata)
    
    loda_data = json.loads(re.sub(b'\x00', b' ', mdata.tobytes()))
    return loda_data

# Example usage:
decoded_data = decode_json_bytes(json_bytes)
print(decoded_data)

# Example usage of the new method:
tobytes_data = tobytes_json_data(json_bytes)
print(tobytes_data)

# 다음은 인코딩된 값 중 모든 0x00 값을 공백으로 대체하는 코드입니다:
    
# 설명
# 데이터 정의: data 딕셔너리에 샘플 데이터를 정의합니다. message 필드와 page_str_list 항목에 널 문자(\x00)를 포함합니다.
# JSON 문자열로 변환: json.dumps(data, ensure_ascii=False)를 사용하여 데이터를 JSON 문자열로 변환합니다. ensure_ascii=False 옵션을 사용하여 비 ASCII 문자가 그대로 출력되도록 합니다.
# 바이트 문자열로 인코딩: json_data.encode('utf-8')를 사용하여 JSON 문자열을 UTF-8로 인코딩하여 바이트 문자열로 변환합니다.
# 널 문자 대체: re.sub(b'\x00', b' ', json_bytes)를 사용하여 바이트 문자열에서 모든 0x00 값을 공백(b' ')으로 대체합니다.
# 결과 출력: 변환된 바이트 문자열을 출력합니다.
# 코드에 적용
# 다음은 인코딩된 값 중 모든 0x00 값을 공백으로 대체하는 전체 코드 예제입니다:
    
#  이 코드는 주어진 데이터를 JSON 문자열로 변환하고, UTF-8로 인코딩하여 바이트 문자열로 변환한 후, 0x00 값을 공백으로 대체합니다. 변환된 바이트 문자열을 출력하여 결과를 확인할 수 있습니다.


# 데이터 정의: data 딕셔너리에 샘플 데이터를 정의합니다. message 필드와 page_str_list 항목에 널 문자(\x00)를 포함합니다.
# JSON 문자열로 변환: json.dumps(data, ensure_ascii=False)를 사용하여 데이터를 JSON 문자열로 변환합니다. ensure_ascii=False 옵션을 사용하여 비 ASCII 문자가 그대로 출력되도록 합니다.
# 바이트 문자열로 인코딩: json_data.encode('utf-8')를 사용하여 JSON 문자열을 UTF-8로 인코딩하여 바이트 문자열로 변환합니다.
# 널 문자 대체: re.sub(b'\x00', b' ', json_bytes)를 사용하여 바이트 문자열에서 모든 0x00 값을 공백(b' ')으로 대체합니다.
# 결과 출력: 변환된 바이트 문자열을 출력합니다.
# decode_json_bytes 함수: 바이트 문자열을 디코딩하여 JSON 객체로 변환하는 함수입니다. re.sub(b'\x00', b' ', encoded_bytes).decode('utf-8')를 사용하여 0x00 값을 공백으로 대체한 후 UTF-8로 디코딩합니다.
# tobytes_json_data 함수: memoryview를 사용하여 바이트 데이터를 반환하는 함수입니다. memoryview(encoded_bytes).tobytes()를 사용하여 바이트 데이터를 반환합니다.
# 예제 사용: decode_json_bytes 함수를 사용하여 디코딩된 데이터를 출력하고, tobytes_json_data 함수를 사용하여 memoryview를 통해 바이트 데이터를 출력합니다.
# 이 코드는 memoryview를 사용하여 바이트 데이터를 처리하는 방법을 보여줍니다. memoryview를 사용하면 데이터를 복사하지 않고도 메모리 버퍼의 내용을 읽거나 쓸 수 있습니다.


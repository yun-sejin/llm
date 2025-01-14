import os

def generate_1gb_html_file(output_path, total_size=1024*1024*1024):
    """
    지정한 경로에 약 1GB 크기의 HTML 파일을 생성합니다.
    
    :param output_path: 생성할 HTML 파일 경로
    :param total_size: 생성할 데이터 크기(기본 1GB)
    """
    print("Generating 1GB HTML file in chunks...")

    # 반복해서 쓸 HTML 청크(대략 수 KB 단위)
    # 필요에 따라 문자열 길이를 조절할 수 있습니다.
    chunk = (
        "<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
        "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris "
        "nisi ut aliquip ex ea commodo consequat.</p>\n"
    )
    chunk_size = len(chunk.encode('utf-8'))  # 실제 바이트 크기
    repeat_count = total_size // chunk_size  # 청크를 몇 번 반복할지
    remainder = total_size % chunk_size      # 나머지 바이트

    with open(output_path, 'w', encoding='utf-8') as f:
        # HTML 시작 태그
        f.write("<html><head><title>1GB Test</title></head><body>\n")

        # 청크 반복 작성
        for i in range(repeat_count):
            f.write(chunk)

        # 나머지가 있다면 잘라서 기록 (크기는 정확히 안 맞지만 대략 1GB에 가깝게 생성)
        if remainder > 0:
            partial_chunk = chunk[:remainder]
            f.write(partial_chunk)

        # HTML 종료 태그
        f.write("</body></html>\n")

    print(f"Finished generating approximately 1GB HTML file at: {output_path}")

if __name__ == "__main__":
    output_html = "/home/luemier/llm/llm/dags/big_test.html"
    generate_1gb_html_file(output_html)
    
    
    
  #참고
  # 
  # https://www.postgresql.org/message-id/CAFj8pRAcfKoiNp2uXeiZOd5kRX29n2ofsoLDh0w6ej7RxKoZyA%40mail.gmail.com
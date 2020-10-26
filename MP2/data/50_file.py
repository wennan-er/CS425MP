
with open('50MB_file', 'w') as f:
    num_chars = 1024 * 1024 * 50
    f.write('1' * num_chars)
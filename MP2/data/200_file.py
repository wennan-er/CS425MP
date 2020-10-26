
with open('500MB_file', 'w') as f:
    num_chars = 1024 * 1024 * 500
    f.write('1' * num_chars)
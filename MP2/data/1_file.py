
with open('1GB_file', 'w') as f:
    num_chars = 1024 * 1024 * 1024
    f.write('1' * num_chars)
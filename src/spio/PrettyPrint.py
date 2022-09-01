def print_key_values(collected_data):
    max_spaces = len(str(len(collected_data))) + 0x1
    for i, pair in enumerate(collected_data):
        spaces = " " * (max_spaces - len(str(i)))
        print(f"{i}:{spaces}\t{pair}")

def get_data_from_cursor(cursor, limit=None):
    batch = []
    cnt = 0

    for item in cursor:
        if limit is not None and cnt >= limit:
            break
        batch.append(item)
        cnt += 1

    return batch

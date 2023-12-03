# from neo4j.work.result import Result
#
# def get_data_from_cursor(cursor: Result, limit=None):
#     batch = []
#     cnt = 0
#     while True:
#         try:
#             if limit is not None and cnt >= limit:
#                 raise StopIteration
#             item = next(cursor)
#             batch.append(item)
#             cnt += 1
#         except StopIteration:
#             return batch

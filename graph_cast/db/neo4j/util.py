def upsert_docs_batch(
    match_keys,
    collection_name,
    # match_keys=None,
    update_keys=None,
    filter_uniques=True,
):
    """
        batch is sent in context
        {batch: [
        {id:"alice@example.com", name:"Alice",age:32},{id:"bob@example.com", name:"Bob",age:42}]}
        UNWIND $batch as row
        MERGE (n:Label {id: row.id})
        (ON CREATE) SET n += row

    :param match_keys: dict of properties
    :param collection_name:

    :return:
    """

    index_str = ", ".join([f"{k}: row.{k}" for k in match_keys])
    q = f"""
        WITH $batch AS batch 
        UNWIND batch as row 
        MERGE (n:{collection_name} {{ {index_str} }}) 
        ON MATCH set n += row 
        ON CREATE set n += row
    """

    return q


def insert_edges_batch(
    # docs_edges,
    # source_collection_name,
    # target_collection_name,
    # edge_col_name,
    # match_keys_source=("_key",),
    # match_keys_target=("_key",),
    # filter_uniques=True,
    # uniq_weight_fields=None,
    # uniq_weight_collections=None,
    # upsert_option=False,
    # head=None,
    # **kwargs,
    source_type,
    target_type,
    source_match,
    target_match,
    rlabel,
):
    source_match_str = [f"source.{key} = row[1].{key}" for key in source_match]
    target_match_str = [f"target.{key} = row[2].{key}" for key in target_match]

    match_clause = "WHERE " + " AND ".join(source_match_str + target_match_str)

    q = f"""
        WITH $batch AS batch 
        UNWIND batch as row 
        MATCH (source:{source_type}), (target:{target_type}) {match_clause} MERGE (source)-[r:{rlabel}]->(target)
    """
    return q

MATCH (m:maintainer)-[r1]-(p:package)
MATCH (p)-[r2]-(b:bug)
OPTIONAL MATCH (p)-[r3]-(p2:package)
RETURN * limit 2000
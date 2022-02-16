CREATE ({test: 1}), ({test: 2});
MATCH (n {test: 1}), (m {test: 2}) CREATE (n)-[:bridgeup]->(m);
MATCH ()-[r:bridgeup]-() RETURN COUNT(r);

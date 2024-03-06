:param {
  idsToSkip: []
};


:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.nodes.officer.csv" AS row
WITH row
WHERE NOT row.`node_id` IN $idsToSkip AND NOT toInteger(trim(row.`node_id`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Officer` { `id`: toInteger(trim(row.`node_id`)) })
  SET n.`id` = toInteger(trim(row.`node_id`))
  SET n.`node_id` = toInteger(trim(row.`node_id`))
  SET n.`name` = row.`name`
  SET n.`country_codes` = row.`country_codes`
  SET n.`countries` = row.`countries`
  SET n.`sourceID` = row.`sourceID`
  SET n.`valid_until` = row.`valid_until`
  SET n.`note` = row.`note`
} IN TRANSACTIONS OF 10000 ROWS;

CREATE CONSTRAINT `imp_uniq_Officer_id` IF NOT EXISTS
FOR (n: `Officer`)
REQUIRE (n.`id`) IS UNIQUE;



:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.nodes.address.csv" AS row
WITH row
WHERE NOT row.`node_id` IN $idsToSkip AND NOT toInteger(trim(row.`node_id`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Addresses` { `id`: toInteger(trim(row.`node_id`)) })
  SET n.`id` = toInteger(trim(row.`node_id`))
  SET n.`name` = row.`name`
  SET n.`address` = row.`address`
  SET n.`country_codes` = row.`country_codes`
  SET n.`countries` = row.`countries`
  SET n.`sourceID` = row.`sourceID`
  SET n.`valid_until` = row.`valid_until`
  SET n.`note` = row.`note`
} IN TRANSACTIONS OF 10000 ROWS;

CREATE CONSTRAINT `imp_uniq_Addresses_id` IF NOT EXISTS
FOR (n: `Addresses`)
REQUIRE (n.`id`) IS UNIQUE;




:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.nodes.entity.csv" AS row
WITH row
WHERE NOT row.`node_id` IN $idsToSkip AND NOT toInteger(trim(row.`node_id`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Entity` { `id`: toInteger(trim(row.`node_id`)) })
  SET n.`id` = toInteger(trim(row.`node_id`))
  SET n.`name` = row.`name`
  SET n.`jurisdiction` = row.`jurisdiction`
  SET n.`jurisdiction_description` = row.`jurisdiction_description`
  SET n.`country_codes` = row.`country_codes`
  SET n.`countries` = row.`countries`
  SET n.`incorporation_date` = row.`incorporation_date`
  SET n.`inactivation_date` = row.`inactivation_date`
  SET n.`struck_off_date` = row.`struck_off_date`
  SET n.`closed_date` = row.`closed_date`
  SET n.`ibcRUC` = row.`ibcRUC`
  SET n.`status` = row.`status`
  SET n.`company_type` = row.`company_type`
  SET n.`service_provider` = row.`service_provider`
  SET n.`sourceID` = row.`sourceID`
  SET n.`valid_until` = row.`valid_until`
  SET n.`note` = row.`note`
} IN TRANSACTIONS OF 10000 ROWS;

CREATE CONSTRAINT `imp_uniq_Entity_id` IF NOT EXISTS
FOR (n: `Entity`)
REQUIRE (n.`id`) IS UNIQUE;



:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.nodes.intermediary.csv" AS row
WITH row
WHERE NOT row.`node_id` IN $idsToSkip AND NOT toInteger(trim(row.`node_id`)) IS NULL
CALL {
  WITH row
  MERGE (n: `Intermediary` { `id`: toInteger(trim(row.`node_id`)) })
  SET n.`id` = toInteger(trim(row.`node_id`))
  SET n.`name` = row.`name`
  SET n.`country_codes` = row.`country_codes`
  SET n.`countries` = row.`countries`
  SET n.`status` = row.`status`
  SET n.`sourceID` = row.`sourceID`
  SET n.`valid_until` = row.`valid_until`
  SET n.`note` = row.`note`
} IN TRANSACTIONS OF 10000 ROWS;

CREATE CONSTRAINT `imp_uniq_Intermediary_id` IF NOT EXISTS
FOR (n: `Intermediary`)
REQUIRE (n.`id`) IS UNIQUE;



:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.edges.csv" AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Officer` { `id`: toInteger(trim(row.`START_ID`)) })
  MATCH (target: `Entity` { `id`: toInteger(trim(row.`END_ID`)) })
  MERGE (source)-[r: `OFFICER_OF`]->(target)
  SET r.`officer_id` = toInteger(trim(row.`START_ID`))
  SET r.`TYPE` = row.`TYPE`
  SET r.`entity_id` = toInteger(trim(row.`END_ID`))
  SET r.`link` = row.`link`
} IN TRANSACTIONS OF 10000 ROWS;


:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.edges.csv" AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Officer` { `id`: toInteger(trim(row.`START_ID`)) })
  MATCH (target: `Intermediary` { `id`: toInteger(trim(row.`END_ID`)) })
  MERGE (source)-[r: `OFFICER_OF`]->(target)
  SET r.`officer_id` = toInteger(trim(row.`START_ID`))
  SET r.`TYPE` = row.`TYPE`
  SET r.`intermediary_id` = toInteger(trim(row.`END_ID`))
  SET r.`link` = row.`link`
} IN TRANSACTIONS OF 10000 ROWS;


:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.edges.csv" AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Officer` { `id`: toInteger(trim(row.`START_ID`)) })
  MATCH (target: `Officer` { `id`: toInteger(trim(row.`END_ID`)) })
  MERGE (source)-[r: `OFFICER_OF`]->(target)
  SET r.`officer_id_1` = toInteger(trim(row.`START_ID`))
  SET r.`TYPE` = row.`TYPE`
  SET r.`officer_id_2` = toInteger(trim(row.`END_ID`))
  SET r.`link` = row.`link`
} IN TRANSACTIONS OF 10000 ROWS;



:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.edges.csv" AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Officer` { `id`: toInteger(trim(row.`START_ID`)) })
  MATCH (target: `Addresses` { `id`: toInteger(trim(row.`END_ID`)) })
  MERGE (source)-[r: `LISTED_ADDRESS`]->(target)
  SET r.`officer_id` = toInteger(trim(row.`START_ID`))
  SET r.`TYPE` = row.`TYPE`
  SET r.`address_id` = toInteger(trim(row.`END_ID`))
  SET r.`link` = row.`link`
} IN TRANSACTIONS OF 10000 ROWS;


:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.edges.csv" AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Entity` { `id`: toInteger(trim(row.`START_ID`)) })
  MATCH (target: `Addresses` { `id`: toInteger(trim(row.`END_ID`)) })
  MERGE (source)-[r: `LISTED_ADDRESS`]->(target)
  SET r.`entity_id` = toInteger(trim(row.`START_ID`))
  SET r.`TYPE` = row.`TYPE`
  SET r.`address_id` = toInteger(trim(row.`END_ID`))
  SET r.`link` = row.`link`
} IN TRANSACTIONS OF 10000 ROWS;

:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.edges.csv" AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Intermediary` { `id`: toInteger(trim(row.`START_ID`)) })
  MATCH (target: `Addresseses` { `id`: toInteger(trim(row.`END_ID`)) })
  MERGE (source)-[r: `LISTED_ADDRESS`]->(target)
  SET r.`intermediary_id` = toInteger(trim(row.`START_ID`))
  SET r.`TYPE` = row.`TYPE`
  SET r.`address_id` = toInteger(trim(row.`END_ID`))
  SET r.`link` = row.`link`
  SET r.`start_date` = row.`start_date`
  SET r.`end_date` = row.`end_date`
  SET r.`sourceID` = row.`sourceID`
  SET r.`valid_until` = row.`valid_until`
} IN TRANSACTIONS OF 10000 ROWS;


:auto LOAD CSV WITH HEADERS FROM "file:///panama_papers.edges.csv" AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `Intermediary` { `id`: toInteger(trim(row.`START_ID`)) })
  MATCH (target: `Entity` { `id`: toInteger(trim(row.`END_ID`)) })
  MERGE (source)-[r: `INTERMEDIARY_OF`]->(target)
  SET r.`intermediary_id` = toInteger(trim(row.`START_ID`))
  SET r.`TYPE` = row.`TYPE`
  SET r.`entity_id` = toInteger(trim(row.`END_ID`))
  SET r.`link` = row.`link`
} IN TRANSACTIONS OF 10000 ROWS;



CREATE INDEX FOR (p:Officer) ON (p.name);
CREATE INDEX FOR (a:Addresses) ON (a.countries);
CREATE INDEX FOR (e:Entity) ON (e.name);

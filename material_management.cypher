// load from csv 
load csv with headers from 'file:///po_vendor2.csv' as row 
merge (p:PO{order_number:row.`Order Number`})
merge (s:Supplier{supplier:row.`Supplier`})
set 
n.name=row.`Supplier Name`
create (m:Material{id:row.`Date Receipt`})
set 
m.short_description=row.`Short Description`,
m.order_number=row.`Order Number`,
m.qty=row.Qty,
m.unit_cost=row.`Unit Cost`
MERGE (p)-[:HAS_MATERIAL]->(m)
merge (s)<-[:PO_ISSUED]-(p);

/// nlp task from material short desc
MATCH (n:Material) 
with  split(n.short_description,',') as text,n
unwind range(0,size(text)-2) as i 
merge (w1:Item{text:text[i]})
merge (w2:Item{text:text[i+1]})
merge (w1)-[r:NEXT_DESC]->(w2)
on create set r.count=1
on match set r.count=r.count+1
merge (w1)<-[:SHORT_DESC]-(n)
merge (w2)<-[:SHORT_DESC]-(n);

// similarity of text context 
// MATCH (s:Item)
// // Get right1, left1
// MATCH (w:Item)-[:NEXT_DESC]->(s)
// WITH collect(DISTINCT w.text) as left1, s
// MATCH (w:Item)<-[:NEXT_DESC]-(s)
// WITH left1, s, collect(DISTINCT w.text) as right1
// // Match every other Item
// MATCH (o:Item) WHERE NOT s = o
// WITH left1, right1, s, o
// // Get other right, other left1
// MATCH (w:Item)-[:NEXT_DESC]->(o)
// WITH collect(DISTINCT w.text) as left1_o, s, o, right1, left1
// MATCH (w:Item)<-[:NEXT_DESC]-(o)
// WITH left1_o, s, o, right1, left1, collect(DISTINCT w.text) as right1_o
// // Compute right1 union, intersect
// WITH apoc.coll.subtract(right1 + right1_o, apoc.coll.subtract(right1, right1_o)) as r1_intersect,
//   right1 + right1_o AS r1_union, s, o, right1, left1, right1_o, left1_o
// // Compute left1 union, intersect
// WITH apoc.coll.subtract(left1 + left1_o, apoc.coll.subtract(left1, left1_o)) as l1_intersect,
//   left1 + left1_o AS l1_union, r1_intersect, r1_union, s, o
// WITH 1.0 * size(r1_intersect) / size(r1_union) as r1_jaccard,
//   1.0 * size(l1_intersect) / size(l1_union) as l1_jaccard,
//   s, o
// WITH s, o, r1_jaccard, l1_jaccard, r1_jaccard + l1_jaccard as sim
// CREATE (s)-[r:RELATED_TO]->(o) SET r.paradig = sim;


/// fraud pattern 1
match path=(s1:Supplier)<--(p:PO)-->(m1:Material)--(i:Item)--(m2:Material)<--(p2:PO)-->(s2:Supplier)
where p<>p2 and s1<>s2
return path limit 100

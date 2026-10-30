import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import {
  MAX_GRAPH_ELEMENTS, validateGraph, searchNodes, neighborhood, subgraph,
} from "../model.mjs";

const source = JSON.parse(readFileSync(new URL("../demo.json",import.meta.url),"utf8"));
const valid = () => validateGraph(structuredClone(source));

test("synthetic fixture is explicitly identified and accepted",() => {
  const graph = valid();
  assert.equal(graph.provenance.dataset_type,"synthetic-demo");
  assert.equal(graph.elements.nodes.length,5);
  assert.equal(graph.elements.edges.length,4);
});
test("search works for UniProt, Rhea master/directional and ChEBI IDs",() => {
  const graph = valid();
  assert.equal(searchNodes(graph,"P00001")[0].data.id,"protein:P00001");
  assert.equal(searchNodes(graph,"RHEA:10000")[0].data.id,"reaction:RHEA:10000");
  assert.equal(searchNodes(graph,"RHEA:10001")[0].data.id,"reaction:RHEA:10000");
  assert.equal(searchNodes(graph,"CHEBI:15377")[0].data.id,"compound:CHEBI:15377");
});
test("depth controls neighborhood; no EC-derived edges",() => {
  const graph = valid();
  const shallow = neighborhood(graph,"protein:P00001",{depth:1});
  assert.equal(shallow.nodes.length,2);
  assert.equal(shallow.edges.length,1);
  const broad = neighborhood(graph,"protein:P00001",{depth:2});
  assert.equal(broad.nodes.length,5);
  assert.equal(broad.edges.length,4);
  assert.equal(broad.edges.filter(e=>e.data.relation==="CATALYZES").length,1);
});
test("evidence and reviewed filters affect only eligible catalytic edges",() => {
  const graph = valid();
  assert.equal(neighborhood(graph,"protein:P00001",{eco:"ECO:0000305"}).edges.length,4);
  assert.equal(neighborhood(graph,"protein:P00001",{eco:"ECO:0000269"}).edges.length,0);
  assert.equal(neighborhood(graph,"protein:P00001",{reviewedOnly:true}).edges.length,4);
  const unreviewed = structuredClone(source);
  unreviewed.elements.nodes[0].data.reviewed=false;
  assert.equal(neighborhood(validateGraph(unreviewed),"reaction:RHEA:10000",
    {reviewedOnly:true}).nodes.length,4);
  assert.throws(() => neighborhood(validateGraph(unreviewed),"protein:P00001",
    {reviewedOnly:true}),/Reviewed/);
});
test("relation toggles never manufacture links",() => {
  const graph=valid();
  const noCatalysis=neighborhood(graph,"protein:P00001",{showCatalysis:false});
  assert.equal(noCatalysis.nodes.length,1);
  const noParticipants=neighborhood(graph,"protein:P00001",{showParticipants:false});
  assert.equal(noParticipants.edges.length,1);
});
test("bounded search and neighborhood reports truncation",() => {
  const graph=valid();
  const view=neighborhood(graph,"reaction:RHEA:10000",{maxNodes:2});
  assert.equal(view.nodes.length,2);
  assert.equal(view.truncated,true);
  assert.ok(view.edges.length<=1);
});
test("subgraph preserves evidence/provenance and records truncation",() => {
  const graph=valid();
  const view=neighborhood(graph,"protein:P00001",{maxNodes:2});
  const sub=subgraph(graph,view);
  assert.equal(sub.provenance.dataset_type,"synthetic-demo");
  assert.equal(sub.view.truncated,true);
  assert.equal(sub.elements.edges.length,1);
  assert.equal(sub.elements.edges[0].data.evidence[0].eco_code,"ECO:0000305");
  assert.doesNotThrow(()=>validateGraph(sub));
});
test("rejects duplicate IDs, dangling edges and invalid relation semantics",() => {
  const duplicate=structuredClone(source);
  duplicate.elements.nodes.push(structuredClone(duplicate.elements.nodes[0]));
  assert.throws(()=>validateGraph(duplicate),/Повторяющийся ID узла/);
  const missing=structuredClone(source);
  missing.elements.edges[0].data.target="reaction:RHEA:99999";
  assert.throws(()=>validateGraph(missing),/Неизвестный конечный узел/);
  const wrong=structuredClone(source);
  wrong.elements.edges[0].data.source="compound:CHEBI:15377";
  assert.throws(()=>validateGraph(wrong),/CATALYZES/);
  const unknown=structuredClone(source);
  unknown.elements.edges[0].data.relation="PREDICTED_CATALYSIS";
  assert.throws(()=>validateGraph(unknown),/Неподдерживаемый тип связи/);
});
test("rejects unsupported schema, invalid IDs and unbounded graphs",() => {
  const old=structuredClone(source);
  old.schema_version="1";
  assert.throws(()=>validateGraph(old),/Неподдерживаемая схема/);
  const bad=structuredClone(source);
  bad.elements.nodes[0].data.id="<script>alert(1)</script>";
  assert.throws(()=>validateGraph(bad),/Неверный ID узла/);
  const large=structuredClone(source);
  large.elements.nodes=Array.from({length:MAX_GRAPH_ELEMENTS+1},(_,i)=>({
    data:{id:"protein:P"+String(i+1).padStart(5,"0"),label:"synthetic"}
  }));
  large.elements.edges=[];
  assert.throws(()=>validateGraph(large),/слишком велик/);
});

test("EC classification and exact taxonomy filters match only annotated proteins",() => {
  const graph=valid();
  assert.equal(searchNodes(graph,"1.11.1.6")[0].data.id,"protein:P00001");
  assert.equal(neighborhood(graph,"protein:P00001",
    {taxonId:"9606",ecNumber:"1.11.1.6"}).edges.length,4);
  assert.equal(neighborhood(graph,"protein:P00001",
    {taxonId:"9606",ecNumber:"EC 1.11.1.6"}).edges.length,4);
  assert.throws(() => neighborhood(graph,"protein:P00001",
    {taxonId:"10090"}),/фильтрам/);
  assert.throws(() => neighborhood(graph,"protein:P00001",
    {ecNumber:"2.7.1.1"}),/фильтрам/);
  assert.throws(() => neighborhood(graph,"protein:P00001",
    {taxonId:"0"}),/положительным/);
  assert.throws(() => neighborhood(graph,"protein:P00001",
    {ecNumber:"1.1.1;DROP"}),/формате/);
  // A taxon filter must not quietly introduce missing evidence links.
  const scoped=neighborhood(graph,"reaction:RHEA:10000",
    {taxonId:"10090"});
  assert.ok(scoped.nodes.every(({data}) => data.node_type !== "Protein"));
  assert.ok(scoped.edges.every(({data}) => data.relation !== "CATALYZES"));
});
test("legacy graphs without taxonomy/EC are still readable and marked unknown",() => {
  const legacy=structuredClone(source);
  for (const node of legacy.elements.nodes) {
    delete node.data.taxon_id;
    delete node.data.ec_numbers;
  }
  const graph=validateGraph(legacy);
  assert.equal(neighborhood(graph,"protein:P00001").edges.length,4);
  assert.throws(() => neighborhood(graph,"protein:P00001",
    {taxonId:"9606",ecNumber:"1.11.1.6"}),/фильтрам/);
});

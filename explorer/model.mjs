/**
 * Pure, dependency-free model for the local Cytoscape.js explorer.
 * The graph is a projection of verified source assertions, never an
 * independent source of evidence or an EC-based protein→reaction join.
 */
export const MAX_FILE_BYTES = 8 * 1024 * 1024;
export const MAX_GRAPH_ELEMENTS = 16000;
export const MAX_VISIBLE_NODES = 120;
export const MAX_VISIBLE_EDGES = 400;
const NODE_ID = /^(protein:[A-Z0-9-]+|reaction:RHEA:[1-9][0-9]*|compound:CHEBI:[1-9][0-9]*)$/;
const EDGE_ID = /^(catalysis|participant):[A-Za-z0-9:_-]+$/;
const RELATIONS = new Set(["CATALYZES", "HAS_PARTICIPANT"]);

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function assert(condition, message) {
  if (!condition) throw new Error(message);
}
function stableId(a, b) {
  return a.data.id.localeCompare(b.data.id, "en");
}

export function validateGraph(document) {
  assert(isObject(document), "Ожидался объект JSON.");
  assert(document.schema_version === "2.0-graph-preview",
    "Неподдерживаемая схема графа: требуется 2.0-graph-preview.");
  const elements = document.elements;
  assert(isObject(elements) && Array.isArray(elements.nodes) && Array.isArray(elements.edges),
    "Отсутствуют elements.nodes или elements.edges.");
  assert(elements.nodes.length + elements.edges.length <= MAX_GRAPH_ELEMENTS,
    "Граф слишком велик для локального просмотрщика. Сузьте экспорт.");
  assert(isObject(document.provenance), "Отсутствует provenance источников.");
  const ids = new Set();
  const nodes = elements.nodes.map((element) => {
    assert(isObject(element) && isObject(element.data), "Недопустимый узел графа.");
    const data = element.data;
    assert(typeof data.id === "string" && NODE_ID.test(data.id), "Неверный ID узла.");
    assert(!ids.has(data.id), "Повторяющийся ID узла: " + data.id);
    assert(typeof data.label === "string", "Отсутствует label узла " + data.id);
    ids.add(data.id);
    return { data };
  });
  const edgesSeen = new Set();
  const edges = elements.edges.map((element) => {
    assert(isObject(element) && isObject(element.data), "Недопустимое ребро графа.");
    const data = element.data;
    assert(typeof data.id === "string" && EDGE_ID.test(data.id), "Неверный ID ребра.");
    assert(!edgesSeen.has(data.id), "Повторяющийся ID ребра: " + data.id);
    assert(typeof data.source === "string" && ids.has(data.source), "Неизвестный исходный узел.");
    assert(typeof data.target === "string" && ids.has(data.target), "Неизвестный конечный узел.");
    assert(RELATIONS.has(data.relation), "Неподдерживаемый тип связи.");
    if (data.relation === "CATALYZES") {
      assert(data.source.startsWith("protein:") && data.target.startsWith("reaction:"),
        "CATALYZES должен соединять Protein и Reaction.");
      assert(Array.isArray(data.evidence), "CATALYZES должен содержать список evidence.");
      assert(/^RHEA:[1-9][0-9]*$/.test(data.rhea_directional_id || ""),
        "Отсутствует Rhea directional ID.");
    } else {
      assert(data.source.startsWith("reaction:") && data.target.startsWith("compound:"),
        "HAS_PARTICIPANT должен соединять Reaction и Compound.");
      assert(data.side === null || data.side === "left" || data.side === "right",
        "Недопустимая сторона реакции.");
    }
    edgesSeen.add(data.id);
    return { data };
  });
  return {
    schema_version: document.schema_version,
    provenance: document.provenance,
    semantics: isObject(document.semantics) ? document.semantics : {},
    ...(typeof document.reference_sha256 === "string"
      ? { reference_sha256: document.reference_sha256 } : {}),
    ...(isObject(document.scope) ? { scope: document.scope } : {}),
    ...(isObject(document.view) ? { view: document.view } : {}),
    elements: {
      nodes: nodes.sort(stableId),
      edges: edges.sort(stableId),
    },
  };
}

export function searchNodes(graph, phrase) {
  const query = String(phrase || "").trim().toLowerCase();
  if (!query) return graph.elements.nodes.slice(0, 40);
  const directionalHits = new Set();
  for (const edge of graph.elements.edges) {
    if (String(edge.data.rhea_directional_id || "").toLowerCase().includes(query)) {
      directionalHits.add(edge.data.target);
    }
  }
  return graph.elements.nodes.filter(({ data }) => {
    const candidates = [data.id, data.label, data.accession,
      data.rhea_master_id, data.chebi_id, data.equation, ...(Array.isArray(data.ec_numbers) ? data.ec_numbers : [])];
    return directionalHits.has(data.id) || candidates.some(
      (candidate) => typeof candidate === "string" && candidate.toLowerCase().includes(query)
    );
  }).slice(0, 80);
}

export function neighborhood(graph, rootId, options = {}) {
  const byId = new Map(graph.elements.nodes.map((entry) => [entry.data.id, entry]));
  assert(byId.has(rootId), "Выбранный узел отсутствует в графе.");
  const depth = options.depth === 1 ? 1 : 2;
  const maxNodes = Math.max(1, Math.min(MAX_VISIBLE_NODES, Number(options.maxNodes) || 80));
  const showCatalysis = options.showCatalysis !== false;
  const showParticipants = options.showParticipants !== false;
  const reviewedOnly = options.reviewedOnly === true;
  const ecoFilter = String(options.eco || "").trim().toUpperCase();
  const taxonId = String(options.taxonId ?? "").trim();
  const ecNumber = String(options.ecNumber || "").trim().toLowerCase().replace(/^ec[ :]+/, "");
  assert(!taxonId || /^[1-9][0-9]*$/.test(taxonId), "Taxon ID должен быть положительным числом.");
  assert(!ecNumber || /^[1-7]\.(?:[0-9]+|-|n[0-9]+)\.(?:[0-9]+|-|n[0-9]+)\.(?:[0-9]+|-|n[0-9]+)$/i.test(ecNumber),
    "Введите EC в формате 1.11.1.6.");
  const acceptedNode = (nodeId) => {
    const data = byId.get(nodeId)?.data;
    if (!data) return false;
    if (data.node_type !== "Protein") return true;
    return (!reviewedOnly || data.reviewed === true)
      && (!taxonId || String(data.taxon_id ?? "") === taxonId)
      && (!ecNumber || (Array.isArray(data.ec_numbers) && data.ec_numbers.some(
        (assigned) => String(assigned).toLowerCase() === ecNumber)));
  };
  assert(acceptedNode(rootId), "Выбранный белок не соответствует фильтрам Reviewed / taxon / EC.");
  const eligible = graph.elements.edges.filter(({ data }) => {
    if (data.relation === "CATALYZES" && !showCatalysis) return false;
    if (data.relation === "HAS_PARTICIPANT" && !showParticipants) return false;
    if (data.relation === "CATALYZES" && ecoFilter) {
      if (!data.evidence.some((ev) => String(ev?.eco_code || "").toUpperCase().includes(ecoFilter))) return false;
    }
    return acceptedNode(data.source) && acceptedNode(data.target);
  });
  const adjacent = new Map();
  for (const edge of eligible) {
    for (const id of [edge.data.source, edge.data.target]) {
      if (!adjacent.has(id)) adjacent.set(id, []);
      adjacent.get(id).push(edge);
    }
  }
  const visited = new Set([rootId]);
  let frontier = [rootId];
  let truncated = false;
  for (let level = 0; level < depth; level++) {
    const next = [];
    for (const id of frontier) {
      const incident = (adjacent.get(id) || []).slice().sort(stableId);
      for (const edge of incident) {
        const other = edge.data.source === id ? edge.data.target : edge.data.source;
        if (visited.has(other)) continue;
        if (visited.size >= maxNodes) {
          truncated = true;
          continue;
        }
        visited.add(other);
        next.push(other);
      }
    }
    frontier = next;
  }
  const nodes = graph.elements.nodes.filter(({ data }) => visited.has(data.id));
  const allEdges = eligible.filter(({ data }) => visited.has(data.source) && visited.has(data.target));
  if (allEdges.length > MAX_VISIBLE_EDGES) truncated = true;
  return {
    nodes,
    edges: allEdges.slice(0, MAX_VISIBLE_EDGES),
    rootId,
    depth,
    truncated,
    options: { showCatalysis, showParticipants, reviewedOnly, eco: ecoFilter,
      taxonId, ecNumber },
  };
}

export function subgraph(graph, view) {
  return {
    schema_version: graph.schema_version,
    semantics: graph.semantics,
    provenance: graph.provenance,
    ...(typeof graph.reference_sha256 === "string"
      ? { reference_sha256: graph.reference_sha256 } : {}),
    ...(isObject(graph.scope) ? { scope: graph.scope } : {}),
    view: {
      root_id: view.rootId,
      depth: view.depth,
      truncated: view.truncated,
      filters: view.options,
      note: "Подграф для просмотра; не источник новых научных утверждений.",
    },
    elements: { nodes: view.nodes, edges: view.edges },
  };
}

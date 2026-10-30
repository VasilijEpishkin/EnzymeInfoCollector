import { MAX_FILE_BYTES, validateGraph, searchNodes, neighborhood, subgraph } from "./model.mjs";

const $ = (id) => document.getElementById(id);
const controls = ["search","depth","node-limit","filter-catalysis","filter-participants",
                  "filter-reviewed","eco","fit","export"];
let graph = null, cy = null, rootId = null, selectedId = null, visible = null, filename = "graph";

function el(tag, cls, value) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (value !== undefined) node.textContent = value === null ? "Не указано" : String(value);
  return node;
}
function status(text) { $("load-status").textContent = text; }
function linkFor(value) {
  if (typeof value !== "string") return null;
  if (/^[A-Z0-9]+(?:-[0-9]+)?$/.test(value))
    return "https://www.uniprot.org/uniprotkb/" + encodeURIComponent(value);
  if (/^RHEA:[1-9][0-9]*$/.test(value))
    return "https://www.rhea-db.org/rhea/" + value.slice(5);
  if (/^CHEBI:[1-9][0-9]*$/.test(value))
    return "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=" + encodeURIComponent(value);
  if (/^(?:PMID:|PubMed:)[1-9][0-9]*$/.test(value))
    return "https://pubmed.ncbi.nlm.nih.gov/" + value.split(":")[1] + "/";
  return null;
}
function addField(target, name, value) {
  const dl = el("dl","field"), dt = el("dt","",name), dd = el("dd");
  const label = value === null || value === undefined || value === ""
    ? "Не указано" : typeof value === "object" ? JSON.stringify(value) : String(value);
  const url = linkFor(label);
  if (url) {
    const a = el("a","",label); a.href = url; a.rel = "noopener noreferrer"; a.target = "_blank";
    dd.append(a);
  } else dd.textContent = label;
  dl.append(dt, dd); target.append(dl);
}
function header(panel, kind, title, description) {
  const box = el("div","info-header");
  box.append(el("span","type-pill",kind), el("h3","",title));
  if (description) box.append(el("p","",description));
  panel.append(box);
}
function provenance(panel, item) {
  if (!item || typeof item !== "object") return;
  panel.append(el("div","detail-divider"), el("h4","","Источник и релиз"));
  for (const key of ["database","release","retrieved_at","sha256","source_url"]) {
    if (Object.prototype.hasOwnProperty.call(item,key)) addField(panel,key,item[key]);
  }
}
function inspect(data, isEdge) {
  const panel = $("inspector-content"); panel.replaceChildren();
  if (!data) {
    panel.append(el("p","placeholder","Выберите узел или связь для просмотра доказательств."));
    return;
  }
  header(panel, isEdge ? data.relation : data.node_type,
    isEdge ? (data.rhea_directional_id || data.reported_chebi_id || data.id) : (data.label || data.id),
    isEdge ? data.source + " → " + data.target : data.id);
  if (isEdge && data.relation === "CATALYZES") {
    addField(panel,"Направленный Rhea ID",data.rhea_directional_id);
    addField(panel,"Направление",data.direction);
    addField(panel,"ID аннотации",data.annotation_id);
    panel.append(el("div","detail-divider"),el("h4","","Доказательства"));
    for (const item of data.evidence) {
      const card = el("div","evidence-item");
      card.append(el("strong","",item?.eco_code || "ECO не указан"));
      if (item?.source_database) card.append(el("p","","Источник: " + item.source_database));
      const citations = Array.isArray(item?.citation_ids) ? item.citation_ids : [];
      if (!citations.length) card.append(el("p","","Публикация не указана; это не отрицательный результат."));
      for (const citation of citations) {
        const url = linkFor(citation);
        if (url) {
          const a = el("a","",citation); a.href = url; a.target = "_blank"; a.rel = "noopener noreferrer";
          card.append(a);
        } else card.append(el("p","",String(citation)));
      }
      panel.append(card);
    }
    provenance(panel,data.source_release);
  } else if (isEdge) {
    addField(panel,"Участник (ChEBI)",data.reported_chebi_id);
    addField(panel,"Основной ChEBI",data.primary_chebi_id);
    addField(panel,"Сторона уравнения",data.side);
    addField(panel,"Коэффициент",data.coefficient);
    panel.append(el("p","empty-note","Левая и правая стороны Rhea не обязаны соответствовать физиологическим субстратам/продуктам."));
  } else {
    if (data.node_type === "Protein") {
      addField(panel,"UniProt accession",data.accession);
      addField(panel,"Reviewed",data.reviewed === true ? "Да (Swiss-Prot)" : "Нет/неизвестно");
      addField(panel,"Версия записи",data.entry_version);
      addField(panel,"Версия последовательности",data.sequence_version);
    } else if (data.node_type === "Reaction") {
      addField(panel,"Основной Rhea ID",data.rhea_master_id);
      addField(panel,"Уравнение",data.equation);
      addField(panel,"Статус",data.status);
    } else {
      addField(panel,"ChEBI ID",data.chebi_id);
      addField(panel,"Формула",data.formula);
      addField(panel,"Заряд",data.charge);
      if (data.node_type === "UnresolvedCompound")
        panel.append(el("p","empty-note","Не удалось разрешить ChEBI ID. Это не доказательство отсутствия соединения."));
    }
    provenance(panel,data.source_release);
    if (visible) {
      const related = visible.edges.filter((edge) => edge.data.source === data.id || edge.data.target === data.id);
      if (related.length) {
        panel.append(el("div","detail-divider"),el("h4","","Связи (" + related.length + ")"));
        for (const edge of related.slice(0,35)) {
          const button = el("button","inline",edge.data.relation + " · " +
            (edge.data.rhea_directional_id || edge.data.reported_chebi_id || edge.data.id));
          button.type = "button";
          button.addEventListener("click",() => selectElement(edge.data.id));
          panel.append(button,document.createTextNode(" "));
        }
      }
    }
  }
}
function options() {
  return {
    depth: Number($("depth").value),
    maxNodes: Number($("node-limit").value),
    showCatalysis: $("filter-catalysis").checked,
    showParticipants: $("filter-participants").checked,
    reviewedOnly: $("filter-reviewed").checked,
    eco: $("eco").value.trim(),
  };
}
function searchResults() {
  const list = $("search-results"); list.replaceChildren();
  if (!graph) return;
  const rows = searchNodes(graph,$("search").value);
  $("results-count").textContent = "Найдено: " + rows.length + (rows.length === 80 ? "+" : "");
  const labels = {Protein:"Белок",Reaction:"Реакция",Compound:"Соединение",UnresolvedCompound:"Не разрешено"};
  for (const entry of rows) {
    const d = entry.data;
    const button = el("button","result-entry" + (rootId === d.id ? " active" : ""));
    button.type = "button";
    const dot = el("span","swatch " + d.node_type);
    dot.setAttribute("aria-hidden","true");
    button.append(dot,el("span","name",d.label || d.id),
      el("span","kind",labels[d.node_type] || "Узел"));
    button.setAttribute("aria-label",d.id);
    button.addEventListener("click",() => {rootId = d.id; selectedId = d.id; render();});
    list.append(button);
  }
  if (!rows.length) list.append(el("p","hint","Ничего не найдено в загруженном графе."));
}
function styles() {
  return [
    {selector:"node",style:{"background-color":"#7695ac","label":"data(label)","font-size":11,
      "font-weight":600,"color":"#dfedf5","text-valign":"bottom","text-margin-y":9,
      "text-wrap":"wrap","text-max-width":120,"width":32,"height":32,
      "border-width":2,"border-color":"#304d60","overlay-opacity":0}},
    {selector:'node[node_type="Protein"]',style:{"background-color":"#69bde9","border-color":"#3786b0","width":44,"height":44}},
    {selector:'node[node_type="Reaction"]',style:{"background-color":"#efbd73","border-color":"#a87938","shape":"round-diamond","width":40,"height":40}},
    {selector:'node[node_type="Compound"]',style:{"background-color":"#73ccb1","border-color":"#328572"}},
    {selector:'node[node_type="UnresolvedCompound"]',style:{"background-color":"#8c8199","border-style":"dashed"}},
    {selector:"edge",style:{"line-color":"#4b748c","width":2,"curve-style":"bezier",
      "target-arrow-shape":"triangle","target-arrow-color":"#4b748c","arrow-scale":0.75}},
    {selector:'edge[relation="CATALYZES"]',style:{"line-color":"#69bde9",
      "target-arrow-color":"#69bde9","width":2.6}},
    {selector:'edge[relation="HAS_PARTICIPANT"]',style:{"line-color":"#75bba6",
      "target-arrow-color":"#75bba6"}},
    {selector:":selected",style:{"border-color":"#fff","border-width":4,
      "line-color":"#e6fbff","target-arrow-color":"#e6fbff","z-index":100}},
  ];
}
function ensureRenderer() {
  if (cy) return cy;
  if (typeof window.cytoscape !== "function")
    throw new Error("Библиотека Cytoscape.js не загружена. Проверьте подключение к CDN.");
  cy = window.cytoscape({
    container: $("graph"), elements: [], style: styles(),
    minZoom:0.12,maxZoom:3,wheelSensitivity:0.18,boxSelectionEnabled:false,
  });
  cy.on("tap","node, edge",(event) => {
    selectedId = event.target.id();
    inspect(event.target.data(),event.target.isEdge());
  });
  return cy;
}
function selectElement(id) {
  if (!cy) return;
  const element = cy.getElementById(id);
  if (element.empty()) return;
  cy.elements().unselect();
  element.select();
  selectedId = id;
  inspect(element.data(),element.isEdge());
}
function render() {
  if (!graph || !rootId) return;
  try {
    visible = neighborhood(graph,rootId,options());
    const viewer = ensureRenderer();
    viewer.elements().remove();
    viewer.add([...visible.nodes,...visible.edges]);
    viewer.layout({name:"cose",animate:false,fit:true,padding:40,randomize:false,numIter:300,
      nodeRepulsion:() => 450000,idealEdgeLength:() => 95}).run();
    $("empty-state").hidden = true;
    $("focus-title").textContent = rootId;
    $("graph-count").textContent = visible.nodes.length + " узлов · " + visible.edges.length + " связей";
    status(graph.elements.nodes.length + " узлов в файле · показано " + visible.nodes.length +
      (visible.truncated ? " · область ограничена" : ""));
    if (selectedId && viewer.getElementById(selectedId).nonempty()) selectElement(selectedId);
    else selectElement(rootId);
    searchResults();
  } catch (error) { status("Ошибка отображения: " + error.message); }
}
async function load(data,name,isDemo) {
  const validated = validateGraph(data);
  ensureRenderer();
  graph = validated;
  filename = name;
  $("dataset-name").textContent = isDemo ? "ДЕМО — синтетические данные" : name;
  $("search").value = ""; $("eco").value = "";
  $("depth").value = "2"; $("node-limit").value = "80";
  $("filter-catalysis").checked = true; $("filter-participants").checked = true;
  $("filter-reviewed").checked = false;
  rootId = graph.elements.nodes.find((n) => n.data.node_type === "Protein")?.data.id ||
           graph.elements.nodes[0]?.data.id || null;
  selectedId = rootId;
  for (const id of controls) $(id).disabled = !rootId;
  if (rootId) render();
  else {
    $("empty-state").hidden = false;
    $("focus-title").textContent = "Граф пуст";
    status("Файл корректен, но не содержит узлов.");
  }
}
function download(name,text) {
  const href = URL.createObjectURL(new Blob([text],{type:"application/json;charset=utf-8"}));
  const a = el("a"); a.href = href; a.download = name;
  document.body.append(a); a.click(); a.remove();
  setTimeout(() => URL.revokeObjectURL(href),1500);
}
$("file-input").addEventListener("change",async (event) => {
  const file = event.target.files?.[0];
  if (!file) return;
  try {
    if (file.size > MAX_FILE_BYTES)
      throw new Error("Файл больше 8 МБ; экспортируйте небольшой граф.");
    await load(JSON.parse(await file.text()),file.name,false);
  } catch (error) { status("Не удалось открыть файл: " + error.message); }
  event.target.value = "";
});
$("demo").addEventListener("click",async () => {
  try {
    const response = await fetch("./demo.json",{cache:"no-store"});
    if (!response.ok) throw new Error("Демо-файл не найден.");
    await load(await response.json(),"demo.json",true);
  } catch (error) { status("Ошибка демо: " + error.message); }
});
$("search").addEventListener("input",searchResults);
for (const id of ["depth","node-limit","filter-catalysis","filter-participants",
                   "filter-reviewed","eco"]) {
  $(id).addEventListener(id === "eco" ? "input" : "change",render);
}
$("fit").addEventListener("click",() => { if (cy) cy.fit(undefined,45); });
$("export").addEventListener("click",() => {
  if (!graph || !visible) return;
  download(filename.replace(/\.json$/i,"") + "-subgraph.json",
    JSON.stringify(subgraph(graph,visible),null,2) + "\n");
});

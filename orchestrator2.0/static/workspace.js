/* 图谱工作台（全屏三栏）：左=业务搜索+Cypher｜中=ECharts 画布｜右=表格抽屉。
 * 依赖 /static/lib/echarts.min.js（已在 index.html 头部加载）。
 * 画布用 ECharts 自带力导向布局（自动适配视野、有动画；拖动节点会牵动
 * 全图重排，这是该布局的固有行为）。
 * 暴露 window.GW = { open, close, toggle, loadEntity } 供聊天卡片联动。 */
(function () {
  'use strict';

  // 与后端 config.py 镜像的可视化限额
  var FIRST_LIMIT = 200;   // 首屏（搜索/Cypher）最多节点
  var EXPAND_LIMIT = 100;  // 双击展开单次新增上限
  var MAX_NODES = 500;     // 画布总节点上限

  var chart = null;
  var labels = [];         // 分类（节点标签）按出现顺序
  var nodes = new Map();   // id -> {id,label,name}
  var edges = new Map();   // id -> {id,type,source,target,directed[,hop]}
  var expanded = new Set();// 已双击展开过的节点 id（重复双击 no-op）
  var focusIds = new Set();// 中心/选中/展开过的节点（标签优先显示）
  var degMap = {};         // id -> 度数（标签重要性）
  var centerId = null;     // 当前加载实体（图谱面板标题 + 重点标签）
  var tableData = null;    // 最近一次查询的表格数据 {columns, rows, count}
  var schemaLoaded = false;

  var PALETTE = ['#6366f1', '#0ea5e9', '#10b981', '#f59e0b', '#ef4444',
                 '#8b5cf6', '#14b8a6', '#f97316', '#ec4899', '#64748b'];

  // 常见英文契约列 -> 中文表头（其余列按用户写的别名原样展示）
  var COLUMN_TITLES = {
    a_type: '起点类型', a_name: '起点名称', a_id: '起点ID',
    b_type: '终点类型', b_name: '终点名称', b_id: '终点ID',
    rel: '关系', _rel: '关系', type: '关系',
    src: '对象', dir: '方向', nb_type: '关联类型', nb_name: '关联名称', hop: '直接/间接',
  };

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return (s == null ? '' : String(s))
      .replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function api(url, body) {
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      return r.json().then(function (d) { return { ok: r.ok, status: r.status, data: d }; })
        .catch(function () { return { ok: false, status: r.status, data: null }; });
    });
  }

  // ---------------- 面板生命周期（全屏覆盖聊天页） ----------------
  // 现场排障：把面板/画布尺寸与 echarts 状态上报到服务日志（/api/clientlog）
  function diag(tag) {
    try {
      var wsR = document.getElementById('workspace').getBoundingClientRect();
      var chR = document.getElementById('ws-chart').getBoundingClientRect();
      var cv = document.querySelector('#ws-chart canvas');
      fetch('/api/clientlog', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: 'diag:' + tag +
            ' ws=' + Math.round(wsR.width) + 'x' + Math.round(wsR.height) +
            ' chart=' + Math.round(chR.width) + 'x' + Math.round(chR.height) +
            ' canvas=' + (cv ? (cv.width + 'x' + cv.height) : 'none') +
            ' echarts=' + (typeof echarts !== 'undefined' ? echarts.version : 'MISSING') +
            ' nodes=' + nodes.size,
          ua: navigator.userAgent,
        }),
      });
    } catch (e) {}
  }

  // 顶栏 对话/图谱 切换按钮状态同步
  function syncViewSwitch(opened) {
    var a = document.getElementById('view-chat');
    var b = document.getElementById('view-graph');
    if (a && b) {
      a.classList.toggle('on', !opened);
      b.classList.toggle('on', opened);
    }
  }

  function open() {
    $('workspace').classList.remove('hidden');
    syncViewSwitch(true);
    setTimeout(function () { ensureChart(); chart.resize(); diag('open'); }, 80);
    loadSchemaHint();
  }
  function close() {
    $('workspace').classList.add('hidden');
    syncViewSwitch(false);
    hideTable();
  }
  function toggle() {
    if ($('workspace').classList.contains('hidden')) open(); else close();
  }

  // ---------------- 表格抽屉（点击就看，不点击不看） ----------------
  function hideTable() {
    $('ws-table-wrap').classList.add('hidden');
    $('ws-table-btn').classList.remove('on');
    if (chart) setTimeout(function () { chart.resize(); }, 60);
  }
  function showTable() {
    if (!tableData) return;
    renderTable(tableData.columns, tableData.rows);
    $('ws-table-wrap').classList.remove('hidden');
    $('ws-table-btn').classList.add('on');
    // 表格弹出后画布区域变窄，echarts 需重算尺寸（其内部 div 是固定像素宽，
    // 不 resize 会溢出盖住表格头部，导致 ✕ 点不到）
    if (chart) setTimeout(function () { chart.resize(); }, 60);
  }
  function setTableData(columns, rows) {
    tableData = { columns: columns, rows: rows };
    tablePage.no = 1;   // 新数据回到第 1 页
    $('ws-table-btn').disabled = false;
  }
  function clearTableData() {
    tableData = null;
    $('ws-table-btn').disabled = true;
    hideTable();
  }

  // 表格分页（一页 10 条，过长的内容横向滚动）
  var tablePage = { no: 1, size: 10 };

  function renderTable(columns, rows) {
    var box = $('ws-table');
    if (!columns || !columns.length) {
      box.innerHTML = '<div class="ws-loading">无数据</div>';
      $('ws-table-count').textContent = '查询结果';
      return;
    }
    var total = (rows || []).length;
    var pages = Math.max(1, Math.ceil(total / tablePage.size));
    if (tablePage.no > pages) tablePage.no = pages;
    if (tablePage.no < 1) tablePage.no = 1;
    var start = (tablePage.no - 1) * tablePage.size;
    var slice = (rows || []).slice(start, start + tablePage.size);
    var head = '<tr>' + columns.map(function (c) {
      var t = COLUMN_TITLES[c] || c;
      return '<th title="' + esc(c) + '">' + esc(t) + '</th>';
    }).join('') + '</tr>';
    var body = slice.map(function (r) {
      return '<tr>' + columns.map(function (c) {
        var v = r[c];
        return '<td title="' + esc(v) + '">' + esc(v == null ? '' : v) + '</td>';
      }).join('') + '</tr>';
    }).join('');
    box.innerHTML = '<table>' + head + body + '</table>';
    // 分页条
    if (total > tablePage.size) {
      var nav = document.createElement('div');
      nav.className = 'ws-table-page';
      nav.innerHTML = '<button data-d="-1"' + (tablePage.no <= 1 ? ' disabled' : '') + '>‹ 上一页</button>' +
        '<span>第 ' + tablePage.no + ' / ' + pages + ' 页</span>' +
        '<button data-d="1"' + (tablePage.no >= pages ? ' disabled' : '') + '>下一页 ›</button>';
      nav.addEventListener('click', function (e) {
        var d = e.target.getAttribute && e.target.getAttribute('data-d');
        if (!d) return;
        tablePage.no += parseInt(d, 10);
        renderTable(columns, rows);
      });
      box.appendChild(nav);
    }
    $('ws-table-count').textContent = '共 ' + total + ' 行' +
      (total > tablePage.size ? ' · 第 ' + tablePage.no + '/' + pages + ' 页' : '');
  }

  // ---------------- 画布（ECharts 力导向布局） ----------------
  function ensureChart() {
    if (chart) return chart;
    chart = echarts.init($('ws-chart'));
    chart.on('click', function (p) {
      if (p.dataType === 'node') {
        showProps(p.data.rawId);
        focusIds.add(p.data.rawId);
        // 点击高亮该节点及直接关联，其余淡化（emphasis.focus=adjacency）
        try {
          chart.dispatchAction({ type: 'downplay' });
          chart.dispatchAction({ type: 'highlight', dataIndex: p.dataIndex });
        } catch (e) {}
      }
    });
    chart.on('dblclick', function (p) {
      if (p.dataType === 'node') expandNode(p.data.rawId);
    });
    window.addEventListener('resize', function () { chart && chart.resize(); });
    return chart;
  }

  function catIndex(label) {
    var i = labels.indexOf(label);
    if (i < 0) { labels.push(label); i = labels.length - 1; }
    return i;
  }

  function reset() {
    nodes.clear(); edges.clear(); expanded.clear(); labels = []; focusIds.clear();
    centerId = null;
    if (chart) chart.clear();
    var lg = $('ws-legend');
    if (lg) lg.classList.add('hidden');
    var cnt = $('ws-graph-count');
    if (cnt) cnt.textContent = '当前展示 0 个节点 · 0 条关系';
  }

  // 标签显隐规则：节点少全部显示；节点多时只显示 中心/选中/高度数 节点，其余悬停看
  var LABEL_ALL_BELOW = 30;
  function shouldLabel(nid) {
    if (nodes.size <= LABEL_ALL_BELOW) return true;
    return focusIds.has(nid) || (degMap[nid] || 0) >= 4;
  }

  function merge(nlist, elist) {
    var added = 0;
    (nlist || []).forEach(function (n) {
      if (n && n.id && !nodes.has(n.id)) { nodes.set(n.id, n); added++; }
    });
    (elist || []).forEach(function (e) {
      if (e && e.id && !edges.has(e.id)) edges.set(e.id, e);
    });
    return added;
  }

  function render() {
    var ch = ensureChart();
    // 保留已有节点位置作为力导向的初始值（展开/重绘时节点不会全部重飞）
    var pos = {};
    try {
      (ch.getOption().series[0].data || []).forEach(function (d) {
        if (d.rawId != null && d.x != null) pos[d.rawId] = { x: d.x, y: d.y };
      });
    } catch (e) {}
    var deg = {};
    edges.forEach(function (e) {
      deg[e.source] = (deg[e.source] || 0) + 1;
      deg[e.target] = (deg[e.target] || 0) + 1;
    });
    degMap = deg;
    var data = [], links = [];
    nodes.forEach(function (n) {
      var isCenter = n.id === centerId;
      var d = {
        id: n.id, rawId: n.id, name: n.name || n.id,
        category: catIndex(n.label || '其他'),
        symbolSize: isCenter ? 40 : 14 + Math.min(22, 4 * (deg[n.id] || 0)),
      };
      if (isCenter) {
        d.label = { fontSize: 13, fontWeight: 600, color: '#1e2433' };
        d.itemStyle = { borderColor: '#6366f1', borderWidth: 2 };
      }
      if (pos[n.id]) { d.x = pos[n.id].x; d.y = pos[n.id].y; }
      data.push(d);
    });
    edges.forEach(function (e) {
      links.push({
        source: e.source, target: e.target,
        value: e.type + (e.hop ? '（' + e.hop + '）' : ''),
        edgeSymbol: e.directed === false ? ['none', 'none'] : ['none', 'arrow'],
        lineStyle: { width: 1.2, opacity: 0.65, curveness: 0.08 },
      });
    });
    ch.setOption({
      tooltip: {
        confine: true,
        formatter: function (p) {
          if (p.dataType === 'edge') return esc(p.data.value || '');
          var n = nodes.get(p.data.rawId) || {};
          return esc(n.label || '') + ' · ' + esc(n.name || '') +
            '<br/><span style="color:#999">双击展开邻接 · 单击看属性</span>';
        },
      },
      legend: [{ show: false, data: labels }],   // 图例自绘（浮动工具块），legend 组件仅存选中状态
      series: [{
        type: 'graph',
        layout: 'force',        // ECharts 力导向：自带动画
        roam: true, draggable: true,
        zoom: 1.05,
        categories: labels.map(function (l, i) {
          return { name: l, itemStyle: { color: PALETTE[i % PALETTE.length] } };
        }),
        label: {
          show: true, position: 'right', fontSize: 11, color: '#555',
          formatter: function (p) {
            if (!shouldLabel(p.data.rawId)) return '';
            var s = p.data.name || '';
            return s.length > 14 ? s.slice(0, 14) + '…' : s;
          },
        },
        emphasis: { focus: 'adjacency', label: { fontSize: 12 } },
        labelLayout: { hideOverlap: true },
        // 拉开布局半径：让图谱主体占画布一半以上（节点少时不再缩成一小团）
        force: { repulsion: 420, edgeLength: [90, 180], gravity: 0.04 },
        edgeSymbolSize: 8,
        data: data, links: links,
      }],
    }, true);
    ch.resize();
    // 工具栏副标题：当前规模
    var cnt = $('ws-graph-count');
    if (cnt) cnt.textContent = '当前展示 ' + nodes.size + ' 个节点 · ' + edges.size + ' 条关系';
    buildLegend();
    diag('render');
  }

  // ---------------- 浮动图例（自绘，两列小圆点，可点击筛选/折叠） ----------------
  function buildLegend() {
    var box = $('ws-legend');
    var body = $('ws-lg-body');
    if (!box || !body || !labels.length) return;
    var sel = {};
    try { sel = (chart.getOption().legend[0] || {}).selected || {}; } catch (e) {}
    box.classList.remove('hidden');
    body.innerHTML = '';
    labels.forEach(function (lb) {
      var chip = document.createElement('button');
      chip.className = 'ws-lg-chip' + (sel[lb] === false ? ' off' : '');
      chip.title = '点击隐藏/显示「' + lb + '」类节点';
      var dot = document.createElement('i');
      dot.style.background = PALETTE[labels.indexOf(lb) % PALETTE.length];
      chip.appendChild(dot);
      chip.appendChild(document.createTextNode(lb));
      chip.addEventListener('click', function () {
        try {
          chart.dispatchAction({ type: 'legendToggleSelect', name: lb });
          chip.classList.toggle('off');
        } catch (e) {}
      });
      body.appendChild(chip);
    });
  }

  // ---------------- 提示 toast（画布底部，自动消失） ----------------
  var noticeTimer = null;
  function notice(msg, isErr) {
    var el = $('ws-trunc');
    if (noticeTimer) { clearTimeout(noticeTimer); noticeTimer = null; }
    if (!msg) { el.classList.add('hidden'); el.textContent = ''; return; }
    el.classList.remove('hidden');
    el.classList.toggle('ws-err', !!isErr);
    el.textContent = msg;
    noticeTimer = setTimeout(function () {
      el.classList.add('hidden');
      noticeTimer = null;
    }, isErr ? 7000 : 4500);
  }

  // ---------------- 搜索 → 出图 ----------------
  function doSearch() {
    var kw = $('ws-kw').value.trim();
    if (!kw) return;
    var box = $('ws-cands');
    box.innerHTML = '<div class="ws-loading">搜索中…</div>';
    api('/graph/business/search', { keyword: kw }).then(function (r) {
      var d = r.data || {};
      var list = d.candidates || [];
      if (!r.ok || !list.length) {
        box.innerHTML = '<div class="ws-loading">' +
          (r.ok ? '未找到候选，换个关键词试试' : '搜索失败：' + esc(d.error || r.status)) + '</div>';
        return;
      }
      box.innerHTML = '';
      list.forEach(function (c) {
        var el = document.createElement('div');
        el.className = 'ws-cand';
        var metas = [];
        for (var k in c) {
          if (['business_type', 'id', 'name', 'matched_on'].indexOf(k) < 0 && c[k] != null)
            metas.push(esc(k) + '：' + esc(c[k]));
        }
        el.innerHTML = '<div><span class="ctype">【' + esc(c.business_type) + '】</span>' +
          '<span class="cname">' + esc(c.name) + '</span></div>' +
          '<div class="cmeta">ID：' + esc(c.id) + (metas.length ? ' · ' + metas.join(' · ') : '') + '</div>';
        el.addEventListener('click', function () {
          loadEntity({ type: c.business_type, id: c.id, name: c.name });
        });
        box.appendChild(el);
      });
    }).catch(function (e) {
      box.innerHTML = '<div class="ws-loading">请求失败：' + esc(e.message) + '</div>';
    });
  }

  function loadEntity(ent) {
    open();
    if (!ent || ent.id == null || ent.id === '') return;
    reset();
    clearTableData();
    // 图谱面板标题：当前展示对象（与对话页"当前分析对象"条同步语义）
    var t = $('ws-entity-name');
    if (t) {
      t.textContent = ent.name || ent.id;
      t.title = ent.type ? ent.type + ' · ' + (ent.name || ent.id) : '';
    }
    notice('正在加载「' + (ent.name || ent.id) + '」的邻接关系…');
    api('/graph/neighbors', { type: ent.type, id: ent.id, limit: FIRST_LIMIT })
      .then(function (r) {
        var d = r.data || {};
        if (!d.success) { notice('查询失败：' + (d.error || r.status), true); return; }
        merge(d.nodes, d.edges);
        if (d.center && d.center.id) { centerId = d.center.id; focusIds.add(centerId); }
        render();
        setTableData(d.columns, d.rows);   // 该实体的邻接表，点「查看表格」可看
        if (d.truncated) {
          notice('「' + (ent.name || ent.id) + '」邻居较多，仅显示前 ' +
            FIRST_LIMIT + ' 个（表格同步截断）');
        }
      })
      .catch(function (e) { notice('请求失败：' + e.message, true); });
  }

  function expandNode(nid) {
    if (expanded.has(nid)) return;
    var i = nid.indexOf(':');
    if (i < 1) return;
    var type = nid.slice(0, i), id = nid.slice(i + 1);
    if (nodes.size >= MAX_NODES) {
      notice('已达画布节点上限（' + MAX_NODES + '），请缩小范围或重新查询', true);
      return;
    }
    expanded.add(nid);
    notice('正在展开「' + ((nodes.get(nid) || {}).name || nid) + '」的邻接…');
    api('/graph/neighbors', { type: type, id: id, limit: EXPAND_LIMIT })
      .then(function (r) {
        var d = r.data || {};
        if (!d.success) {
          expanded.delete(nid);
          notice('展开失败：' + (d.error || r.status), true);
          return;
        }
        var added = merge(d.nodes, d.edges);
        focusIds.add(nid);   // 展开的节点标签保持显示
        render();
        if (d.rows) setTableData(d.columns, d.rows);   // 表格切到该节点的邻接表
        notice('「' + ((nodes.get(nid) || {}).name || nid) + '」新增 ' + added + ' 个节点' +
          (d.truncated ? '（邻居超 ' + EXPAND_LIMIT + ' 已截断）' : ''));
      })
      .catch(function (e) {
        expanded.delete(nid);
        notice('请求失败：' + e.message, true);
      });
  }

  // ---------------- 节点属性 + 智能分析 ----------------
  function showProps(nid) {
    var i = nid.indexOf(':');
    if (i < 1) return;
    var type = nid.slice(0, i), id = nid.slice(i + 1);
    var box = $('ws-props');
    box.classList.remove('hidden');
    box.innerHTML = '<h5>加载中…<button data-x>✕</button></h5>';
    box.querySelector('button').onclick = function () { box.classList.add('hidden'); };
    // 顺手取该节点的邻接表：单击后「查看表格」即可看它的关联（与属性面板同一节点）
    api('/graph/neighbors', { type: type, id: id, limit: 100 }).then(function (r) {
      var d = r.data || {};
      if (d.success && d.rows) setTableData(d.columns, d.rows);
    }).catch(function () {});
    api('/graph/node', { type: type, id: id }).then(function (r) {
      var d = r.data || {};
      if (!d.success) {
        box.innerHTML = '<h5>属性查询失败<button data-x>✕</button></h5><div style="font-size:12px;color:#b42318">' +
          esc(d.error || r.status) + '</div>';
        box.querySelector('button').onclick = function () { box.classList.add('hidden'); };
        return;
      }
      var name = d.name || id;
      var rows = '';
      var props = d.props || {};
      for (var k in props) {
        rows += '<tr><td class="k">' + esc(k) + '</td><td class="v">' +
          esc(Array.isArray(props[k]) ? props[k].join('、') : props[k]) + '</td></tr>';
      }
      box.innerHTML = '<h5>' + esc(d.label || type) + ' · ' + esc(name) +
        '<button data-x>✕</button></h5><table>' + rows + '</table>' +
        '<div class="ws-analyze-actions"><button class="ws-continue-btn">↩ 以此业务继续分析</button></div>' +
        '<div style="font-size:11px;color:#999;margin-top:6px">关联表格已载入（右上「查看表格」）· 双击节点可展开邻接</div>';
      box.querySelector('button').onclick = function () { box.classList.add('hidden'); };
      box.querySelector('.ws-continue-btn').onclick = function () {
        close();   // 返回对话页（滚动位置与未发送内容保持原样）
        if (window.sendFromWorkspace) {
          window.sendFromWorkspace('分析「' + name + '」变更的影响');
        }
      };
    }).catch(function (e) {
      box.innerHTML = '<h5>请求失败<button data-x>✕</button></h5>';
      box.querySelector('button').onclick = function () { box.classList.add('hidden'); };
    });
  }

  // 注：节点级"智能分析"已并入对话链路——「以此业务继续分析」返回对话页跑完整影响分析

  // ---------------- 手写 Cypher ----------------
  function runCypher() {
    var c = $('ws-cypher-input').value.trim();
    if (!c) return;
    notice('执行查询中…');
    api('/graph/query', { cypher: c, mode: 'graph' }).then(function (r) {
      var d = r.data || {};
      if (!d.success) { notice('查询失败：' + (d.error || r.status), true); return; }
      setTableData(d.columns, d.rows);   // 表格数据备好，按钮可点开看
      if (d.render_hint === 'graph') {
        reset();
        merge(d.nodes, d.edges);
        render();
        notice('已绘制 ' + (d.nodes || []).length + ' 个节点 / ' +
          (d.edges || []).length + ' 条边，点右上「查看表格」看原始行' +
          (d.truncated ? '（结果超限已截断）' : ''));
      } else {
        showTable();   // 画不了的直接展示表格
        notice('返回 ' + d.count + ' 行（该结果结构不支持画图，已用表格展示）' +
          (d.truncated ? '，结果已达上限被截断' : ''));
      }
    }).catch(function (e) { notice('请求失败：' + e.message, true); });
  }

  // ---------------- Cypher 生成（描述 → 语句，复制到查询框自己执行） ----------------
  function genCypher() {
    var q = $('ws-gen-input').value.trim();
    var btn = $('ws-gen-btn');
    var out = $('ws-gen-out');
    var acts = document.querySelector('.ws-gen-actions');
    if (!q || btn.disabled) return;
    btn.disabled = true;
    var old = btn.textContent;
    btn.textContent = '生成中…';
    out.classList.remove('hidden');
    acts.classList.add('hidden');
    out.textContent = '正在按图结构生成 Cypher…';
    api('/graph/cypher/generate', { question: q }).then(function (r) {
      var d = r.data || {};
      btn.disabled = false;
      btn.textContent = old;
      if (!d.success) {
        out.textContent = '生成失败：' + (d.error || r.status);
        return;
      }
      out.textContent = d.cypher;
      acts.classList.remove('hidden');
    }).catch(function (e) {
      btn.disabled = false;
      btn.textContent = old;
      out.textContent = '请求失败：' + e.message;
    });
  }

  function copyGenCypher() {
    var out = $('ws-gen-out');
    var text = out.textContent || '';
    if (!text || text.indexOf('MATCH') < 0) return;
    // 参数内联成字面量，用户复制后可直接执行
    $('ws-cypher-input').value = text;
    $('ws-cypher-input').focus();
    notice('已复制到查询框，点「执行查询」运行');
  }

  // ---------------- schema 提示（可查的节点类型） ----------------
  function loadSchemaHint() {
    if (schemaLoaded) return;
    schemaLoaded = true;
    fetch('/graph/schema').then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) {
        if (!d || !d.node_types) return;
        var names = d.node_types.map(function (n) { return n.name; });
        var el = $('ws-labels');
        if (el) el.textContent = names.join('、');
      }).catch(function () {});
  }

  // ---------------- 事件绑定 ----------------
  document.addEventListener('DOMContentLoaded', function () {
    $('ws-close').addEventListener('click', close);
    $('ws-table-btn').addEventListener('click', function () {
      if ($('ws-table-wrap').classList.contains('hidden')) showTable(); else hideTable();
    });
    $('ws-table-close').addEventListener('click', hideTable);
    $('ws-fit-btn').addEventListener('click', function () {
      if (!chart) return;
      try { chart.dispatchAction({ type: 'restore' }); } catch (e) {}
      chart.resize();
    });
    // 全屏图谱面板
    $('ws-full-btn').addEventListener('click', function () {
      var p = $('ws-graph-panel');
      if (document.fullscreenElement) {
        if (document.exitFullscreen) document.exitFullscreen();
      } else if (p.requestFullscreen) {
        p.requestFullscreen();
      }
    });
    document.addEventListener('fullscreenchange', function () {
      setTimeout(function () { chart && chart.resize(); }, 150);
    });
    // 帮助说明浮层
    $('ws-help-pop').innerHTML = '· 单击节点：高亮该节点及直接关联，其余淡化<br>' +
      '· 双击节点：展开邻接（保持已有布局）<br>' +
      '· 滚轮缩放 · 拖拽平移 · 拖动节点调整位置<br>' +
      '· 图例圆点：点击隐藏/显示某类节点<br>' +
      '· 手动调整视野后不会自动复位，点「适应画布」恢复';
    $('ws-help-btn').addEventListener('click', function (e) {
      e.stopPropagation();
      $('ws-help-pop').classList.toggle('hidden');
    });
    document.addEventListener('click', function (e) {
      var pop = $('ws-help-pop');
      if (!pop.classList.contains('hidden') &&
          !pop.contains(e.target) && e.target !== $('ws-help-btn')) {
        pop.classList.add('hidden');
      }
    });
    // 图例折叠
    $('ws-lg-head').addEventListener('click', function () {
      var body = $('ws-lg-body');
      var folded = body.classList.toggle('hidden');
      $('ws-lg-toggle').textContent = folded ? '展开' : '收起';
    });
    // 右下角缩放工具
    var zoomTools = document.querySelector('.ws-zoom-tools');
    if (zoomTools) {
      zoomTools.addEventListener('click', function (e) {
        var z = e.target.getAttribute && e.target.getAttribute('data-z');
        if (!z || !chart) return;
        if (z === 'fit') {
          try { chart.dispatchAction({ type: 'restore' }); } catch (err) {}
          chart.resize();
        } else {
          try {
            chart.dispatchAction({
              type: 'graphRoam',
              zoom: z === 'in' ? 1.25 : 0.8,
            });
          } catch (err) {}
        }
      });
    }
    // 左侧页签切换
    var tabBtns = document.querySelectorAll('.ws-tabs button');
    for (var i = 0; i < tabBtns.length; i++) {
      tabBtns[i].addEventListener('click', function () {
        for (var j = 0; j < tabBtns.length; j++) tabBtns[j].classList.remove('on');
        this.classList.add('on');
        var pages = document.querySelectorAll('.ws-tabpage');
        for (var k = 0; k < pages.length; k++) {
          pages[k].classList.toggle('hidden', pages[k].id !== this.getAttribute('data-tab'));
        }
      });
    }
    $('ws-search-btn').addEventListener('click', doSearch);
    $('ws-kw').addEventListener('keydown', function (e) {
      if (e.key === 'Enter') doSearch();
    });
    $('ws-run').addEventListener('click', runCypher);
    $('ws-gen-btn').addEventListener('click', genCypher);
    $('ws-gen-copy').addEventListener('click', copyGenCypher);
  });

  window.GW = { open: open, close: close, toggle: toggle, loadEntity: loadEntity };
})();

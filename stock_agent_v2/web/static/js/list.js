// 시장 탭 + 종목 검색 + universe/portfolio 관리 — 클라이언트 사이드.
(function () {
  const search = document.getElementById('search');
  const tabs   = document.querySelectorAll('.filter-bar .tab[data-market]');
  const blocks = document.querySelectorAll('.market-block');
  if (!search || !blocks.length) return;

  let activeTab = 'all';

  function update() {
    const term = (search.value || '').trim().toLowerCase();
    blocks.forEach(b => {
      const market = b.dataset.market;
      const tabMatch = activeTab === 'all' || activeTab === market;
      b.style.display = tabMatch ? '' : 'none';
      if (!tabMatch) return;

      let visible = 0;
      b.querySelectorAll('.ticker-card').forEach(c => {
        if (c.dataset.removed === '1') { c.style.display = 'none'; return; }
        const text = (c.dataset.search || '').toLowerCase();
        const match = !term || text.includes(term);
        c.style.display = match ? '' : 'none';
        if (match) visible++;
      });
      const empty = b.querySelector('.search-empty');
      if (empty) empty.style.display = (visible === 0 && term) ? '' : 'none';
    });
  }

  tabs.forEach(t => {
    t.addEventListener('click', () => {
      tabs.forEach(x => x.classList.remove('active'));
      t.classList.add('active');
      activeTab = t.dataset.market;
      update();
    });
  });

  search.addEventListener('input', update);

  // '/' 키로 검색창 포커스
  document.addEventListener('keydown', e => {
    if (e.key === '/' && document.activeElement !== search
        && document.activeElement.tagName !== 'INPUT') {
      e.preventDefault();
      search.focus();
    }
  });

  // ── 카운트 갱신 ─────────────────────────────────────────
  function recountBlocks() {
    let total = 0;
    blocks.forEach(b => {
      const n = b.querySelectorAll('.ticker-card:not([data-removed="1"])').length;
      const cnt = b.querySelector('.block-cnt');
      if (cnt) cnt.textContent = n;
      total += n;
      // 빈 블록 메시지 처리
      const market = b.dataset.market;
      const emptyMsg = b.querySelector('.' + market + '-empty');
      if (emptyMsg) emptyMsg.style.display = (n === 0) ? '' : 'none';
    });
    // 탭 카운트
    const map = { all: total };
    blocks.forEach(b => {
      map[b.dataset.market] = b.querySelectorAll('.ticker-card:not([data-removed="1"])').length;
    });
    document.querySelectorAll('.filter-bar .tab[data-market]').forEach(t => {
      const cnt = t.querySelector('.cnt');
      if (cnt && map[t.dataset.market] !== undefined) cnt.textContent = map[t.dataset.market];
    });
  }

  // ── 별표 토글 (포트폴리오 편입/제외) ────────────────────
  // 윈도우-레벨 capture phase 에 등록 — 일부 지갑 확장(SES/MetaMask 등)이
  // document 레벨에서 stopPropagation 으로 클릭을 가로채는 케이스에서도
  // 우리 핸들러가 가장 먼저 실행되게.
  function _applyStar(btn, active) {
    btn.classList.toggle('active', active);
    btn.textContent = active ? '⭐' : '☆';
    btn.setAttribute('aria-pressed', active ? 'true' : 'false');
    btn.title = active ? '포트폴리오에서 제외' : '포트폴리오에 편입';
  }

  async function _handleStarClick(e) {
    const btn = e.target && e.target.closest && e.target.closest('.star-btn');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    if (typeof e.stopImmediatePropagation === 'function') {
      e.stopImmediatePropagation();
    }
    if (btn.dataset.busy === '1') return;
    btn.dataset.busy = '1';
    btn.disabled = true;
    const wasActive = btn.classList.contains('active');
    _applyStar(btn, !wasActive);    // optimistic — 즉시 반영
    const ticker = btn.dataset.ticker;
    try {
      const r = await fetch(`/api/portfolio/${encodeURIComponent(ticker)}/toggle`, {
        method: 'POST',
      });
      if (!r.ok) throw new Error(await r.text());
      const data = await r.json();
      _applyStar(btn, data.in_portfolio);
    } catch (err) {
      _applyStar(btn, wasActive);
      alert('별표 토글 실패: ' + err.message);
    } finally {
      btn.disabled = false;
      setTimeout(() => { btn.dataset.busy = '0'; }, 300);
    }
  }
  // capture: true — 모든 다른 click 핸들러(확장 포함)보다 먼저 실행
  window.addEventListener('click', _handleStarClick, { capture: true });

  // 새 카드 추가 시 사용 (현재는 노옵 — window-level capture 가 자동 처리)
  window._bindStarButton = function () { /* no-op */ };

  // ── X 제거 (universe 에서 삭제) ─────────────────────────
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('.remove-btn');
    if (!btn) return;
    e.preventDefault();
    e.stopPropagation();
    const card = btn.closest('.ticker-card');
    const ticker = btn.dataset.ticker;
    const name = card?.querySelector('.name')?.textContent?.trim() || ticker;
    if (!confirm(`'${ticker} ${name}' 를 universe 에서 제거하시겠습니까?\n(포트폴리오에 있으면 함께 제외)`)) return;
    btn.disabled = true;
    try {
      const r = await fetch(`/api/universe/${encodeURIComponent(ticker)}/remove`, {
        method: 'POST',
      });
      if (!r.ok) throw new Error(await r.text());
      if (card) {
        card.dataset.removed = '1';
        card.style.display = 'none';
      }
      recountBlocks();
      update();
    } catch (err) {
      alert('제거 실패: ' + err.message);
      btn.disabled = false;
    }
  });

  // ── 종목 추가 폼 ────────────────────────────────────────
  const addToggle = document.getElementById('toggle-add');
  const addForm   = document.getElementById('add-ticker-form');
  const addMsg    = addForm?.querySelector('.add-msg');

  if (addToggle && addForm) {
    addToggle.addEventListener('click', () => {
      addForm.hidden = !addForm.hidden;
      addToggle.classList.toggle('active', !addForm.hidden);
      if (!addForm.hidden) addForm.querySelector('input[name="ticker"]').focus();
    });
  }

  if (addForm) {
    addForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(addForm);
      const payload = {
        ticker:   (fd.get('ticker')   || '').toString().trim().toUpperCase(),
        name:     (fd.get('name')     || '').toString().trim(),
        exchange: (fd.get('exchange') || '').toString().trim(),
      };
      if (!payload.ticker) return;
      addMsg.className = 'add-msg';
      addMsg.textContent = '추가 중…';
      try {
        const r = await fetch('/api/universe/add', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload),
        });
        const data = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(data.detail || r.statusText);
        addMsg.className = 'add-msg ok';
        addMsg.textContent = `✓ ${data.ticker} ${data.name} 추가됨 — 별표를 눌러 포트폴리오에 편입할 수 있습니다.`;
        addForm.reset();
        // 카드 추가 (해당 시장 블록 그리드 맨 앞)
        const block = document.querySelector(`.market-block[data-market="${data.is_overseas ? 'us' : 'kr'}"]`);
        if (block) {
          const grid = block.querySelector('.ticker-grid');
          const card = buildCard(data);
          grid.insertBefore(card, grid.firstChild);
          // 새 카드의 별표 버튼에도 핸들러 바인딩
          const newStar = card.querySelector('.star-btn');
          if (newStar) bindStarButton(newStar);
        }
        recountBlocks();
        update();
      } catch (err) {
        addMsg.className = 'add-msg error';
        addMsg.textContent = '✗ ' + err.message;
      }
    });
  }

  function buildCard(d) {
    // 분석 없음 (no-analysis) 상태로 카드 생성. 분석 생기면 다음 페이지 갱신때 has-analysis 로.
    const card = document.createElement('div');
    card.className = 'ticker-card no-analysis';
    card.dataset.search = (d.ticker + ' ' + d.name).toLowerCase();
    card.dataset.ticker = d.ticker;
    card.innerHTML = `
      <button type="button" class="star-btn ${d.is_portfolio ? 'active' : ''}"
              data-ticker="${d.ticker}"
              title="${d.is_portfolio ? '포트폴리오에서 제외' : '포트폴리오에 편입'}"
              aria-pressed="${d.is_portfolio ? 'true' : 'false'}">${d.is_portfolio ? '⭐' : '☆'}</button>
      <div class="card-link">
        <div class="row1">
          <span class="ticker">${escapeHtml(d.ticker)}</span>
          <span class="name">${escapeHtml(d.name)}</span>
        </div>
        <div class="row2"><span class="when muted">분석 없음</span></div>
      </div>
      <button type="button" class="remove-btn" data-ticker="${d.ticker}"
              title="universe 에서 제거">×</button>
    `;
    return card;
  }

  function escapeHtml(s) {
    return String(s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
  }
})();
